/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import org.signal.chat.common.EcPreKey;
import org.signal.chat.common.EcSignedPreKey;
import org.signal.chat.common.KemSignedPreKey;
import org.signal.chat.keys.GetPreKeysResponse;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

class KeysGrpcHelper {

  static final byte ALL_DEVICES = 0;

  static Mono<GetPreKeysResponse> getPreKeys(final Account targetAccount,
      final ServiceIdentifier targetServiceIdentifier,
      final byte targetDeviceId,
      final KeysManager keysManager) {

    final Flux<Device> devices = targetDeviceId == ALL_DEVICES
        ? Flux.fromIterable(targetAccount.getDevices())
        : Flux.from(Mono.justOrEmpty(targetAccount.getDevice(targetDeviceId)));

    final String userAgent = RequestAttributesUtil.getUserAgent().orElse(null);
    return devices
        .flatMap(device -> Mono
            .fromFuture(keysManager.takeDevicePreKeys(device.getId(), targetServiceIdentifier, userAgent))
            .flatMap(Mono::justOrEmpty)
            .map(devicePreKeys -> {
              final GetPreKeysResponse.PreKeyBundle.Builder builder = GetPreKeysResponse.PreKeyBundle.newBuilder()
                  .setEcSignedPreKey(EcSignedPreKey.newBuilder()
                      .setKeyId(devicePreKeys.ecSignedPreKey().keyId())
                      .setPublicKey(ByteString.copyFrom(devicePreKeys.ecSignedPreKey().serializedPublicKey()))
                      .setSignature(ByteString.copyFrom(devicePreKeys.ecSignedPreKey().signature()))
                      .build())
                  .setKemOneTimePreKey(KemSignedPreKey.newBuilder()
                      .setKeyId(devicePreKeys.kemSignedPreKey().keyId())
                      .setPublicKey(ByteString.copyFrom(devicePreKeys.kemSignedPreKey().serializedPublicKey()))
                      .setSignature(ByteString.copyFrom(devicePreKeys.kemSignedPreKey().signature()))
                      .build());
              devicePreKeys.ecPreKey().ifPresent(ecPreKey -> builder.setEcOneTimePreKey(EcPreKey.newBuilder()
                    .setKeyId(ecPreKey.keyId())
                    .setPublicKey(ByteString.copyFrom(ecPreKey.serializedPublicKey()))
                    .build()));
              // Cast device IDs to `int` to match data types in the response object’s protobuf definition
              return Tuples.of((int) device.getId(), builder.build());
            }))
        // If there were no devices with valid prekey bundles in the account, the account is gone
        .switchIfEmpty(Mono.error(Status.NOT_FOUND.asException()))
        .collectMap(Tuple2::getT1, Tuple2::getT2)
        .map(preKeyBundles -> GetPreKeysResponse.newBuilder()
            .setIdentityKey(ByteString
                .copyFrom(targetAccount.getIdentityKey(targetServiceIdentifier.identityType())
                .serialize()))
            .putAllPreKeys(preKeyBundles)
            .build());
  }
}
