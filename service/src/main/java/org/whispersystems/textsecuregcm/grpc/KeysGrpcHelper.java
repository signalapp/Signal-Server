/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import org.signal.chat.common.EcPreKey;
import org.signal.chat.common.EcSignedPreKey;
import org.signal.chat.common.KemSignedPreKey;
import org.signal.chat.keys.AccountPreKeyBundles;
import org.signal.chat.keys.DevicePreKeyBundle;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import java.util.Optional;

class KeysGrpcHelper {

  static final byte ALL_DEVICES = 0;

  /**
   * Fetch {@link AccountPreKeyBundles} from the targetAccount
   *
   * @param targetAccount The targetAccount to fetch pre-key bundles from
   * @param targetServiceIdentifier The serviceIdentifier used to lookup the targetAccount
   * @param targetDeviceId The deviceId to retrieve pre-key bundles for, or ALL_DEVICES if all devices should be retrieved
   * @param keysManager The {@link KeysManager} to lookup pre-keys from
   * @return The requested bundles, or an empty Mono if the keys for the targetAccount do not exist
   */
  static Mono<AccountPreKeyBundles> getPreKeys(final Account targetAccount,
      final ServiceIdentifier targetServiceIdentifier,
      final byte targetDeviceId,
      final KeysManager keysManager) {

    final Flux<Device> devices = targetDeviceId == ALL_DEVICES
        ? Flux.fromIterable(targetAccount.getDevices())
        : Flux.from(Mono.justOrEmpty(targetAccount.getDevice(targetDeviceId)));

    final String userAgent = RequestAttributesUtil.getUserAgent().orElse(null);
    return devices
        .flatMap(device -> {
          final int registrationId = device.getRegistrationId(targetServiceIdentifier.identityType());
          return Mono
              .fromFuture(keysManager.takeDevicePreKeys(device.getId(), targetServiceIdentifier, userAgent))
              .flatMap(Mono::justOrEmpty)
              .map(devicePreKeys -> {
                final DevicePreKeyBundle.Builder builder = DevicePreKeyBundle.newBuilder()
                    .setEcSignedPreKey(EcSignedPreKey.newBuilder()
                        .setKeyId(devicePreKeys.ecSignedPreKey().keyId())
                        .setPublicKey(ByteString.copyFrom(devicePreKeys.ecSignedPreKey().serializedPublicKey()))
                        .setSignature(ByteString.copyFrom(devicePreKeys.ecSignedPreKey().signature()))
                        .build())
                    .setKemOneTimePreKey(KemSignedPreKey.newBuilder()
                        .setKeyId(devicePreKeys.kemSignedPreKey().keyId())
                        .setPublicKey(ByteString.copyFrom(devicePreKeys.kemSignedPreKey().serializedPublicKey()))
                        .setSignature(ByteString.copyFrom(devicePreKeys.kemSignedPreKey().signature()))
                        .build())
                    .setRegistrationId(registrationId);
                devicePreKeys.ecPreKey().ifPresent(ecPreKey -> builder.setEcOneTimePreKey(EcPreKey.newBuilder()
                    .setKeyId(ecPreKey.keyId())
                    .setPublicKey(ByteString.copyFrom(ecPreKey.serializedPublicKey()))
                    .build()));
                // Cast device IDs to `int` to match data types in the response object’s protobuf definition
                return Tuples.of((int) device.getId(), builder.build());
              });
        })
        .collectMap(Tuple2::getT1, Tuple2::getT2)
        .flatMap(preKeyBundles -> {
          if (preKeyBundles.isEmpty()) {
            // If there were no devices with valid prekey bundles in the account, the account is gone
            return Mono.empty();
          }

          final IdentityKey targetIdentityKey = targetAccount.getIdentityKey(targetServiceIdentifier.identityType());
          return Mono.just(AccountPreKeyBundles.newBuilder()
              .setIdentityKey(ByteString.copyFrom(targetIdentityKey.serialize()))
              .putAllDevicePreKeys(preKeyBundles)
              .build());
        });
  }
}
