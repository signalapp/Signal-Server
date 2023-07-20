/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.util.UUID;
import org.signal.chat.common.EcPreKey;
import org.signal.chat.common.EcSignedPreKey;
import org.signal.chat.common.KemSignedPreKey;
import org.signal.chat.common.ServiceIdentifier;
import org.signal.chat.keys.GetPreKeysResponse;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

class KeysGrpcHelper {

  @VisibleForTesting
  static final long ALL_DEVICES = 0;

  static Mono<Account> findAccount(final ServiceIdentifier targetIdentifier, final AccountsManager accountsManager) {

    return Mono.just(IdentityType.fromGrpcIdentityType(targetIdentifier.getIdentityType()))
        .flatMap(identityType -> {
          final UUID uuid = UUIDUtil.fromByteString(targetIdentifier.getUuid());

          return Mono.fromFuture(switch (identityType) {
            case ACI -> accountsManager.getByAccountIdentifierAsync(uuid);
            case PNI -> accountsManager.getByPhoneNumberIdentifierAsync(uuid);
          });
        })
        .flatMap(Mono::justOrEmpty)
        .onErrorMap(IllegalArgumentException.class, throwable -> Status.INVALID_ARGUMENT.asException());
  }

  static Tuple2<UUID, IdentityKey> getIdentifierAndIdentityKey(final Account account, final IdentityType identityType) {
    final UUID identifier = switch (identityType) {
      case ACI -> account.getUuid();
      case PNI -> account.getPhoneNumberIdentifier();
    };

    final IdentityKey identityKey = switch (identityType) {
      case ACI -> account.getIdentityKey();
      case PNI -> account.getPhoneNumberIdentityKey();
    };

    return Tuples.of(identifier, identityKey);
  }

  static Mono<GetPreKeysResponse> getPreKeys(final Account targetAccount, final IdentityType identityType, final long targetDeviceId, final KeysManager keysManager) {
    final Tuple2<UUID, IdentityKey> identifierAndIdentityKey = getIdentifierAndIdentityKey(targetAccount, identityType);

    final Flux<Device> devices = targetDeviceId == ALL_DEVICES
        ? Flux.fromIterable(targetAccount.getDevices())
        : Flux.from(Mono.justOrEmpty(targetAccount.getDevice(targetDeviceId)));

    return devices
        .filter(Device::isEnabled)
        .switchIfEmpty(Mono.error(Status.NOT_FOUND.asException()))
        .flatMap(device -> Mono.zip(Mono.fromFuture(keysManager.takeEC(identifierAndIdentityKey.getT1(), device.getId())),
                Mono.fromFuture(keysManager.takePQ(identifierAndIdentityKey.getT1(), device.getId())))
            .map(oneTimePreKeys -> {
              final ECSignedPreKey ecSignedPreKey = switch (identityType) {
                case ACI -> device.getSignedPreKey();
                case PNI -> device.getPhoneNumberIdentitySignedPreKey();
              };

              final GetPreKeysResponse.PreKeyBundle.Builder preKeyBundleBuilder = GetPreKeysResponse.PreKeyBundle.newBuilder()
                  .setEcSignedPreKey(EcSignedPreKey.newBuilder()
                      .setKeyId(ecSignedPreKey.keyId())
                      .setPublicKey(ByteString.copyFrom(ecSignedPreKey.serializedPublicKey()))
                      .setSignature(ByteString.copyFrom(ecSignedPreKey.signature()))
                      .build());

              oneTimePreKeys.getT1().ifPresent(ecPreKey -> preKeyBundleBuilder.setEcOneTimePreKey(EcPreKey.newBuilder()
                  .setKeyId(ecPreKey.keyId())
                  .setPublicKey(ByteString.copyFrom(ecPreKey.serializedPublicKey()))
                  .build()));

              oneTimePreKeys.getT2().ifPresent(kemSignedPreKey -> preKeyBundleBuilder.setKemOneTimePreKey(KemSignedPreKey.newBuilder()
                  .setKeyId(kemSignedPreKey.keyId())
                  .setPublicKey(ByteString.copyFrom(kemSignedPreKey.serializedPublicKey()))
                  .setSignature(ByteString.copyFrom(kemSignedPreKey.signature()))
                  .build()));

              return Tuples.of(device.getId(), preKeyBundleBuilder.build());
            }))
        .collectMap(Tuple2::getT1, Tuple2::getT2)
        .map(preKeyBundles -> GetPreKeysResponse.newBuilder()
            .setIdentityKey(ByteString.copyFrom(identifierAndIdentityKey.getT2().serialize()))
            .putAllPreKeys(preKeyBundles)
            .build());
  }
}
