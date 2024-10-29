/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.signal.chat.common.EcPreKey;
import org.signal.chat.common.EcSignedPreKey;
import org.signal.chat.common.KemSignedPreKey;
import org.signal.chat.keys.GetPreKeyCountRequest;
import org.signal.chat.keys.GetPreKeyCountResponse;
import org.signal.chat.keys.GetPreKeysRequest;
import org.signal.chat.keys.GetPreKeysResponse;
import org.signal.chat.keys.ReactorKeysGrpc;
import org.signal.chat.keys.SetEcSignedPreKeyRequest;
import org.signal.chat.keys.SetKemLastResortPreKeyRequest;
import org.signal.chat.keys.SetOneTimeEcPreKeysRequest;
import org.signal.chat.keys.SetOneTimeKemSignedPreKeysRequest;
import org.signal.chat.keys.SetPreKeyResponse;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.protocol.kem.KEMPublicKey;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

public class KeysGrpcService extends ReactorKeysGrpc.KeysImplBase {

  private final AccountsManager accountsManager;
  private final KeysManager keysManager;
  private final RateLimiters rateLimiters;

  private static final StatusRuntimeException INVALID_PUBLIC_KEY_EXCEPTION = Status.fromCode(Status.Code.INVALID_ARGUMENT)
      .withDescription("Invalid public key")
      .asRuntimeException();

  private static final StatusRuntimeException INVALID_SIGNATURE_EXCEPTION = Status.fromCode(Status.Code.INVALID_ARGUMENT)
      .withDescription("Invalid signature")
      .asRuntimeException();

  private enum PreKeyType {
    EC,
    KEM
  }

  public KeysGrpcService(final AccountsManager accountsManager,
      final KeysManager keysManager,
      final RateLimiters rateLimiters) {

    this.accountsManager = accountsManager;
    this.keysManager = keysManager;
    this.rateLimiters = rateLimiters;
  }

  @Override
  public Mono<GetPreKeyCountResponse> getPreKeyCount(final GetPreKeyCountRequest request) {
    return Mono.fromSupplier(AuthenticationUtil::requireAuthenticatedDevice)
        .flatMap(authenticatedDevice -> Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
            .map(maybeAccount -> maybeAccount
                .map(account -> Tuples.of(account, authenticatedDevice.deviceId()))
                .orElseThrow(Status.UNAUTHENTICATED::asRuntimeException)))
        .flatMapMany(accountAndDeviceId -> Flux.just(
            Tuples.of(IdentityType.ACI, accountAndDeviceId.getT1().getUuid(), accountAndDeviceId.getT2()),
            Tuples.of(IdentityType.PNI, accountAndDeviceId.getT1().getPhoneNumberIdentifier(), accountAndDeviceId.getT2())
        ))
        .flatMap(identityTypeUuidAndDeviceId -> Flux.merge(
            Mono.fromFuture(() -> keysManager.getEcCount(identityTypeUuidAndDeviceId.getT2(), identityTypeUuidAndDeviceId.getT3()))
                .map(ecKeyCount -> Tuples.of(identityTypeUuidAndDeviceId.getT1(), PreKeyType.EC, ecKeyCount)),

            Mono.fromFuture(() -> keysManager.getPqCount(identityTypeUuidAndDeviceId.getT2(), identityTypeUuidAndDeviceId.getT3()))
                .map(ecKeyCount -> Tuples.of(identityTypeUuidAndDeviceId.getT1(), PreKeyType.KEM, ecKeyCount))
        ))
        .reduce(GetPreKeyCountResponse.newBuilder(), (builder, tuple) -> {
          final IdentityType identityType = tuple.getT1();
          final PreKeyType preKeyType = tuple.getT2();
          final int count = tuple.getT3();

          switch (identityType) {
            case ACI -> {
              switch (preKeyType) {
                case EC -> builder.setAciEcPreKeyCount(count);
                case KEM -> builder.setAciKemPreKeyCount(count);
              }
            }
            case PNI -> {
              switch (preKeyType) {
                case EC -> builder.setPniEcPreKeyCount(count);
                case KEM -> builder.setPniKemPreKeyCount(count);
              }
            }
          }

          return builder;
        })
        .map(GetPreKeyCountResponse.Builder::build);
  }

  @Override
  public Mono<GetPreKeysResponse> getPreKeys(final GetPreKeysRequest request) {
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

    return rateLimiters.getPreKeysLimiter().validateReactive(rateLimitKey)
        .then(Mono.fromFuture(() -> accountsManager.getByServiceIdentifierAsync(targetIdentifier))
            .flatMap(Mono::justOrEmpty))
        .switchIfEmpty(Mono.error(Status.NOT_FOUND.asException()))
        .flatMap(targetAccount ->
            KeysGrpcHelper.getPreKeys(targetAccount, targetIdentifier.identityType(), deviceId, keysManager));
  }

  @Override
  public Mono<SetPreKeyResponse> setOneTimeEcPreKeys(final SetOneTimeEcPreKeysRequest request) {
    return Mono.fromSupplier(AuthenticationUtil::requireAuthenticatedDevice)
        .flatMap(authenticatedDevice -> storeOneTimePreKeys(authenticatedDevice.accountIdentifier(),
            request.getPreKeysList(),
            IdentityTypeUtil.fromGrpcIdentityType(request.getIdentityType()),
            (requestPreKey, ignored) -> checkEcPreKey(requestPreKey),
            (identifier, preKeys) -> keysManager.storeEcOneTimePreKeys(identifier, authenticatedDevice.deviceId(), preKeys)));
  }

  @Override
  public Mono<SetPreKeyResponse> setOneTimeKemSignedPreKeys(final SetOneTimeKemSignedPreKeysRequest request) {
    return Mono.fromSupplier(AuthenticationUtil::requireAuthenticatedDevice)
        .flatMap(authenticatedDevice -> storeOneTimePreKeys(authenticatedDevice.accountIdentifier(),
            request.getPreKeysList(),
            IdentityTypeUtil.fromGrpcIdentityType(request.getIdentityType()),
            KeysGrpcService::checkKemSignedPreKey,
            (identifier, preKeys) -> keysManager.storeKemOneTimePreKeys(identifier, authenticatedDevice.deviceId(), preKeys)));
  }

  private <K, R> Mono<SetPreKeyResponse> storeOneTimePreKeys(final UUID authenticatedAccountUuid,
      final List<R> requestPreKeys,
      final IdentityType identityType,
      final BiFunction<R, IdentityKey, K> extractPreKeyFunction,
      final BiFunction<UUID, List<K>, CompletableFuture<Void>> storeKeysFunction) {

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedAccountUuid))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .map(account -> {
          final List<K> preKeys = requestPreKeys.stream()
              .map(requestPreKey -> extractPreKeyFunction.apply(requestPreKey, account.getIdentityKey(identityType)))
              .toList();

          if (preKeys.isEmpty()) {
            throw Status.INVALID_ARGUMENT.asRuntimeException();
          }

          return Tuples.of(account.getIdentifier(identityType), preKeys);
        })
        .flatMap(identifierAndPreKeys -> Mono.fromFuture(() -> storeKeysFunction.apply(identifierAndPreKeys.getT1(), identifierAndPreKeys.getT2())))
        .thenReturn(SetPreKeyResponse.newBuilder().build());
  }

  @Override
  public Mono<SetPreKeyResponse> setEcSignedPreKey(final SetEcSignedPreKeyRequest request) {
    return Mono.fromSupplier(AuthenticationUtil::requireAuthenticatedDevice)
        .flatMap(authenticatedDevice -> storeRepeatedUseKey(authenticatedDevice.accountIdentifier(),
            request.getIdentityType(),
            request.getSignedPreKey(),
            KeysGrpcService::checkEcSignedPreKey,
            (account, signedPreKey) -> {
              final IdentityType identityType = IdentityTypeUtil.fromGrpcIdentityType(request.getIdentityType());
              final UUID identifier = account.getIdentifier(identityType);

              return Mono.fromFuture(() -> keysManager.storeEcSignedPreKeys(identifier, authenticatedDevice.deviceId(), signedPreKey));
            }));
  }

  @Override
  public Mono<SetPreKeyResponse> setKemLastResortPreKey(final SetKemLastResortPreKeyRequest request) {
    return Mono.fromSupplier(AuthenticationUtil::requireAuthenticatedDevice)
        .flatMap(authenticatedDevice -> storeRepeatedUseKey(authenticatedDevice.accountIdentifier(),
            request.getIdentityType(),
            request.getSignedPreKey(),
            KeysGrpcService::checkKemSignedPreKey,
            (account, lastResortKey) -> {
              final UUID identifier =
                  account.getIdentifier(IdentityTypeUtil.fromGrpcIdentityType(request.getIdentityType()));

              return Mono.fromFuture(() -> keysManager.storePqLastResort(identifier, authenticatedDevice.deviceId(), lastResortKey));
            }));
  }

  private <K, R> Mono<SetPreKeyResponse> storeRepeatedUseKey(final UUID authenticatedAccountUuid,
      final org.signal.chat.common.IdentityType identityType,
      final R storeKeyRequest,
      final BiFunction<R, IdentityKey, K> extractKeyFunction,
      final BiFunction<Account, K, Mono<?>> storeKeyFunction) {

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedAccountUuid))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .map(account -> {
          final IdentityKey identityKey = account.getIdentityKey(IdentityTypeUtil.fromGrpcIdentityType(identityType));
          final K key = extractKeyFunction.apply(storeKeyRequest, identityKey);

          return Tuples.of(account, key);
        })
        .flatMap(accountAndKey -> storeKeyFunction.apply(accountAndKey.getT1(), accountAndKey.getT2()))
        .thenReturn(SetPreKeyResponse.newBuilder().build());
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
}
