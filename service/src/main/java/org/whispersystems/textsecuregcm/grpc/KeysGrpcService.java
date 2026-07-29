/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
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
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.controllers.RateLimitKeys;
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

    final UUID aci = account.getAccountIdentifier();

    final CompletableFuture<Integer> aciEcKeyCountFuture =
        keysManager.getEcCount(aci, authenticatedDevice.deviceId());

    final CompletableFuture<Integer> pniEcKeyCountFuture = account.getPhoneNumberIdentifierOptional()
        .map(pni -> keysManager.getEcCount(pni, authenticatedDevice.deviceId()))
        .orElseGet(() -> CompletableFuture.completedFuture(0));

    final CompletableFuture<Integer> aciKemKeyCountFuture =
        keysManager.getPqCount(aci, authenticatedDevice.deviceId());

    final CompletableFuture<Integer> pniKemKeyCountFuture = account.getPhoneNumberIdentifierOptional()
        .map(pni -> keysManager.getPqCount(pni, authenticatedDevice.deviceId()))
        .orElseGet(() -> CompletableFuture.completedFuture(0));

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
        GrpcServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getTargetIdentifier());


    final Optional<Account> maybeTargetAccount = accountsManager.getByServiceIdentifier(targetIdentifier);

    final byte deviceId = request.hasDeviceId()
        ? DeviceIdUtil.validate(request.getDeviceId())
        : KeysGrpcHelper.ALL_DEVICES;

    final Optional<Integer> targetRegistrationId = maybeTargetAccount
        .filter(_ -> request.hasDeviceId())
        .flatMap(targetAccount -> targetAccount.getDevice(deviceId))
        .map(device -> device.getRegistrationId(targetIdentifier.identityType()));

    final String rateLimitKey = RateLimitKeys.preKeyLimiterKey(
        authenticatedDevice.accountIdentifier(),
        authenticatedDevice.deviceId(),
        targetIdentifier,
        Optional.ofNullable(request.hasDeviceId() ? deviceId : null),
        targetRegistrationId);

    rateLimiters.getPreKeysLimiter().validate(rateLimitKey);

    return maybeTargetAccount
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
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    storeOneTimePreKeys(authenticatedDevice.accountIdentifier(),
        request.getPreKeysList(),
        IdentityTypeUtil.fromGrpcIdentityType(request.getIdentityType()),
        (requestPreKey, _) -> KeysGrpcHelper.checkEcPreKey(requestPreKey, INVALID_PUBLIC_KEY_EXCEPTION),
        (identifier, preKeys) -> keysManager.storeEcOneTimePreKeys(identifier, authenticatedDevice.deviceId(), preKeys));

    return SetPreKeyResponse.getDefaultInstance();
  }

  @Override
  public SetPreKeyResponse setOneTimeKemSignedPreKeys(final SetOneTimeKemSignedPreKeysRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    storeOneTimePreKeys(authenticatedDevice.accountIdentifier(),
        request.getPreKeysList(),
        IdentityTypeUtil.fromGrpcIdentityType(request.getIdentityType()),
        (preKey, identityKey) -> KeysGrpcHelper.checkKemSignedPreKey(preKey, identityKey, INVALID_PUBLIC_KEY_EXCEPTION, INVALID_SIGNATURE_EXCEPTION),
        (identifier, preKeys) -> keysManager.storeKemOneTimePreKeys(identifier, authenticatedDevice.deviceId(), preKeys));

    return SetPreKeyResponse.getDefaultInstance();
  }

  private <K, R> void storeOneTimePreKeys(final UUID authenticatedAccountUuid,
      final List<R> requestPreKeys,
      final IdentityType identityType,
      final BiFunction<R, IdentityKey, K> extractPreKeyFunction,
      final BiFunction<UUID, List<K>, CompletableFuture<Void>> storeKeysFunction) {

    final Account account = getAuthenticatedAccount(authenticatedAccountUuid);

    final UUID identifier = getIdentifier(account, identityType);

    final List<K> preKeys = requestPreKeys.stream()
        .map(requestPreKey -> extractPreKeyFunction.apply(requestPreKey, account.getIdentityKey(identityType)))
        .toList();

    storeKeysFunction.apply(identifier, preKeys).join();
  }

  @Override
  public SetPreKeyResponse setEcSignedPreKey(final SetEcSignedPreKeyRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    storeRepeatedUseKey(authenticatedDevice.accountIdentifier(),
        IdentityTypeUtil.fromGrpcIdentityType(request.getIdentityType()),
        request.getSignedPreKey(),
        (preKey, identityKey) -> KeysGrpcHelper.checkEcSignedPreKey(preKey, identityKey, INVALID_PUBLIC_KEY_EXCEPTION, INVALID_SIGNATURE_EXCEPTION),
        (identifier, signedPreKey) -> keysManager.storeEcSignedPreKeys(identifier, authenticatedDevice.deviceId(), signedPreKey));

    return SetPreKeyResponse.getDefaultInstance();
  }

  @Override
  public SetPreKeyResponse setKemLastResortPreKey(final SetKemLastResortPreKeyRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    storeRepeatedUseKey(authenticatedDevice.accountIdentifier(),
        IdentityTypeUtil.fromGrpcIdentityType(request.getIdentityType()),
        request.getSignedPreKey(),
        (preKey, identityKey) -> KeysGrpcHelper.checkKemSignedPreKey(preKey, identityKey, INVALID_PUBLIC_KEY_EXCEPTION, INVALID_SIGNATURE_EXCEPTION),
        (identifier, lastResortKey) -> keysManager.storePqLastResort(identifier, authenticatedDevice.deviceId(), lastResortKey));

    return SetPreKeyResponse.getDefaultInstance();
  }

  private <K, R> void storeRepeatedUseKey(final UUID authenticatedAccountUuid,
      final IdentityType identityType,
      final R storeKeyRequest,
      final BiFunction<R, IdentityKey, K> extractKeyFunction,
      final BiFunction<UUID, K, CompletableFuture<Void>> storeKeyFunction) {

    final Account account = getAuthenticatedAccount(authenticatedAccountUuid);

    final UUID identifier = getIdentifier(account, identityType);
    final IdentityKey identityKey = account.getIdentityKey(identityType);
    final K key = extractKeyFunction.apply(storeKeyRequest, identityKey);

    storeKeyFunction.apply(identifier, key).join();
  }

  private Account getAuthenticatedAccount(final UUID authenticatedAccountId) {
    return accountsManager.getByAccountIdentifier(authenticatedAccountId)
        .orElseThrow(() -> GrpcExceptions.invalidCredentials("invalid credentials"));
  }

  /// Get the identity of the requested `idenityType`, or throw an invalid arguments status if `account` does not
  /// contain that type.
  private static UUID getIdentifier(Account account, IdentityType identityType) {
    return switch (identityType) {
      case ACI -> account.getAccountIdentifier();
      case PNI -> account.getPhoneNumberIdentifierOptional()
          .orElseThrow(() -> GrpcExceptions.invalidArguments("PNI identity type not allowed for an account without a phone number"));
    };
  }

}
