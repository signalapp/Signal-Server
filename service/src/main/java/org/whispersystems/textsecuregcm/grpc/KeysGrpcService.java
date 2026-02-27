/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.signal.chat.common.EcPreKey;
import org.signal.chat.common.EcSignedPreKey;
import org.signal.chat.common.KemSignedPreKey;
import org.signal.chat.errors.NotFound;
import org.signal.chat.keys.GetPreKeyCountRequest;
import org.signal.chat.keys.GetPreKeyCountResponse;
import org.signal.chat.keys.GetPreKeysRequest;
import org.signal.chat.keys.GetPreKeysResponse;
import org.signal.chat.keys.SetEcSignedPreKeyRequest;
import org.signal.chat.keys.SetKemLastResortPreKeyRequest;
import org.signal.chat.keys.SetOneTimeEcPreKeysRequest;
import org.signal.chat.keys.SetOneTimeKemSignedPreKeysRequest;
import org.signal.chat.keys.SetPreKeyResponse;
import org.signal.chat.keys.SimpleKeysGrpc;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.protocol.kem.KEMPublicKey;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.KeysManager;

public class KeysGrpcService extends SimpleKeysGrpc.KeysImplBase {

  private final AccountsManager accountsManager;
  private final KeysManager keysManager;
  private final RateLimiters rateLimiters;

  private static final StatusRuntimeException INVALID_PUBLIC_KEY_EXCEPTION =
      GrpcExceptions.fieldViolation("pre_keys", "invalid public key");

  private static final StatusRuntimeException INVALID_SIGNATURE_EXCEPTION =
      GrpcExceptions.fieldViolation("pre_keys", "pre-key signature did not match account identity key");

  public KeysGrpcService(final AccountsManager accountsManager,
      final KeysManager keysManager,
      final RateLimiters rateLimiters) {

    this.accountsManager = accountsManager;
    this.keysManager = keysManager;
    this.rateLimiters = rateLimiters;
  }

  @Override
  public GetPreKeyCountResponse getPreKeyCount(final GetPreKeyCountRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    final Account account = getAuthenticatedAccount(authenticatedDevice.accountIdentifier());

    final CompletableFuture<Integer> aciEcKeyCountFuture =
        keysManager.getEcCount(account.getIdentifier(IdentityType.ACI), authenticatedDevice.deviceId());

    final CompletableFuture<Integer> pniEcKeyCountFuture =
        keysManager.getEcCount(account.getIdentifier(IdentityType.PNI), authenticatedDevice.deviceId());

    final CompletableFuture<Integer> aciKemKeyCountFuture =
        keysManager.getPqCount(account.getIdentifier(IdentityType.ACI), authenticatedDevice.deviceId());

    final CompletableFuture<Integer> pniKemKeyCountFuture =
        keysManager.getPqCount(account.getIdentifier(IdentityType.PNI), authenticatedDevice.deviceId());

    CompletableFuture.allOf(aciEcKeyCountFuture, pniEcKeyCountFuture, aciKemKeyCountFuture, pniKemKeyCountFuture).join();

    return GetPreKeyCountResponse.newBuilder()
        .setAciEcPreKeyCount(aciEcKeyCountFuture.resultNow())
        .setPniEcPreKeyCount(pniEcKeyCountFuture.resultNow())
        .setAciKemPreKeyCount(aciKemKeyCountFuture.resultNow())
        .setPniKemPreKeyCount(pniKemKeyCountFuture.resultNow())
        .build();
  }

  @Override
  public GetPreKeysResponse getPreKeys(final GetPreKeysRequest request) throws RateLimitExceededException {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    final ServiceIdentifier targetIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getTargetIdentifier());

    final byte deviceId = request.hasDeviceId()
        ? DeviceIdUtil.validate(request.getDeviceId())
        : KeysGrpcHelper.ALL_DEVICES;

    final String rateLimitKey = authenticatedDevice.accountIdentifier() + "." +
        authenticatedDevice.deviceId() + "__" +
        targetIdentifier.uuid() + "." +
        deviceId;

    rateLimiters.getPreKeysLimiter().validate(rateLimitKey);

    return accountsManager.getByServiceIdentifier(targetIdentifier)
        .flatMap(targetAccount -> KeysGrpcHelper.getPreKeys(targetAccount, targetIdentifier, deviceId, keysManager))
        .map(accountPreKeyBundles -> GetPreKeysResponse.newBuilder()
            .setPreKeys(accountPreKeyBundles)
            .build())
        .orElseGet(() -> GetPreKeysResponse.newBuilder()
            .setTargetNotFound(NotFound.getDefaultInstance())
            .build());
  }

  @Override
  public SetPreKeyResponse setOneTimeEcPreKeys(final SetOneTimeEcPreKeysRequest request) {
    if (request.getPreKeysList().isEmpty()) {
      throw GrpcExceptions.fieldViolation("pre_keys", "pre_keys must be non-empty");
    }

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    storeOneTimePreKeys(authenticatedDevice.accountIdentifier(),
        request.getPreKeysList(),
        IdentityTypeUtil.fromGrpcIdentityType(request.getIdentityType()),
        (requestPreKey, _) -> checkEcPreKey(requestPreKey),
        (identifier, preKeys) -> keysManager.storeEcOneTimePreKeys(identifier, authenticatedDevice.deviceId(), preKeys));

    return SetPreKeyResponse.getDefaultInstance();
  }

  @Override
  public SetPreKeyResponse setOneTimeKemSignedPreKeys(final SetOneTimeKemSignedPreKeysRequest request) {
    if (request.getPreKeysList().isEmpty()) {
      throw GrpcExceptions.fieldViolation("pre_keys", "pre_keys must be non-empty");
    }

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    storeOneTimePreKeys(authenticatedDevice.accountIdentifier(),
        request.getPreKeysList(),
        IdentityTypeUtil.fromGrpcIdentityType(request.getIdentityType()),
        KeysGrpcService::checkKemSignedPreKey,
        (identifier, preKeys) -> keysManager.storeKemOneTimePreKeys(identifier, authenticatedDevice.deviceId(), preKeys));

    return SetPreKeyResponse.getDefaultInstance();
  }

  private <K, R> void storeOneTimePreKeys(final UUID authenticatedAccountUuid,
      final List<R> requestPreKeys,
      final IdentityType identityType,
      final BiFunction<R, IdentityKey, K> extractPreKeyFunction,
      final BiFunction<UUID, List<K>, CompletableFuture<Void>> storeKeysFunction) {

    final Account account = getAuthenticatedAccount(authenticatedAccountUuid);

    final List<K> preKeys = requestPreKeys.stream()
        .map(requestPreKey -> extractPreKeyFunction.apply(requestPreKey, account.getIdentityKey(identityType)))
        .toList();

    storeKeysFunction.apply(account.getIdentifier(identityType), preKeys).join();
  }

  @Override
  public SetPreKeyResponse setEcSignedPreKey(final SetEcSignedPreKeyRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    storeRepeatedUseKey(authenticatedDevice.accountIdentifier(),
        request.getIdentityType(),
        request.getSignedPreKey(),
        KeysGrpcService::checkEcSignedPreKey,
        (account, signedPreKey) -> {
          final IdentityType identityType = IdentityTypeUtil.fromGrpcIdentityType(request.getIdentityType());
          final UUID identifier = account.getIdentifier(identityType);

          return keysManager.storeEcSignedPreKeys(identifier, authenticatedDevice.deviceId(), signedPreKey);
        });

    return SetPreKeyResponse.getDefaultInstance();
  }

  @Override
  public SetPreKeyResponse setKemLastResortPreKey(final SetKemLastResortPreKeyRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    storeRepeatedUseKey(authenticatedDevice.accountIdentifier(),
        request.getIdentityType(),
        request.getSignedPreKey(),
        KeysGrpcService::checkKemSignedPreKey,
        (account, lastResortKey) -> {
          final UUID identifier =
              account.getIdentifier(IdentityTypeUtil.fromGrpcIdentityType(request.getIdentityType()));

          return keysManager.storePqLastResort(identifier, authenticatedDevice.deviceId(), lastResortKey);
        });

    return SetPreKeyResponse.getDefaultInstance();
  }

  private <K, R> void storeRepeatedUseKey(final UUID authenticatedAccountUuid,
      final org.signal.chat.common.IdentityType identityType,
      final R storeKeyRequest,
      final BiFunction<R, IdentityKey, K> extractKeyFunction,
      final BiFunction<Account, K, CompletableFuture<Void>> storeKeyFunction) {

    final Account account = getAuthenticatedAccount(authenticatedAccountUuid);

    final IdentityKey identityKey = account.getIdentityKey(IdentityTypeUtil.fromGrpcIdentityType(identityType));
    final K key = extractKeyFunction.apply(storeKeyRequest, identityKey);

    storeKeyFunction.apply(account, key).join();
  }

  private static ECPreKey checkEcPreKey(final EcPreKey preKey) {
    try {
      return new ECPreKey(preKey.getKeyId(), new ECPublicKey(preKey.getPublicKey().toByteArray()));
    } catch (final InvalidKeyException e) {
      throw INVALID_PUBLIC_KEY_EXCEPTION;
    }
  }

  private static ECSignedPreKey checkEcSignedPreKey(final EcSignedPreKey preKey, final IdentityKey identityKey) {
    try {
      final ECSignedPreKey ecSignedPreKey = new ECSignedPreKey(preKey.getKeyId(),
          new ECPublicKey(preKey.getPublicKey().toByteArray()),
          preKey.getSignature().toByteArray());

      if (ecSignedPreKey.signatureValid(identityKey)) {
        return ecSignedPreKey;
      } else {
        throw INVALID_SIGNATURE_EXCEPTION;
      }
    } catch (final InvalidKeyException e) {
      throw INVALID_PUBLIC_KEY_EXCEPTION;
    }
  }

  private static KEMSignedPreKey checkKemSignedPreKey(final KemSignedPreKey preKey, final IdentityKey identityKey) {
    try {
      final KEMSignedPreKey kemSignedPreKey = new KEMSignedPreKey(preKey.getKeyId(),
          new KEMPublicKey(preKey.getPublicKey().toByteArray()),
          preKey.getSignature().toByteArray());

      if (kemSignedPreKey.signatureValid(identityKey)) {
        return kemSignedPreKey;
      } else {
        throw INVALID_SIGNATURE_EXCEPTION;
      }
    } catch (final InvalidKeyException e) {
      throw INVALID_PUBLIC_KEY_EXCEPTION;
    }
  }

  private Account getAuthenticatedAccount(final UUID authenticatedAccountId) {
    return accountsManager.getByAccountIdentifier(authenticatedAccountId)
        .orElseThrow(() -> GrpcExceptions.invalidCredentials("invalid credentials"));
  }
}
