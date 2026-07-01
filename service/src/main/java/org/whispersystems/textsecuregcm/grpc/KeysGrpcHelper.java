/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import io.grpc.StatusRuntimeException;
import org.signal.chat.common.EcPreKey;
import org.signal.chat.common.EcSignedPreKey;
import org.signal.chat.common.KemSignedPreKey;
import org.signal.chat.keys.AccountPreKeyBundles;
import org.signal.chat.keys.DevicePreKeyBundle;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.protocol.kem.KEMPublicKey;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessChecksum;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.KeyIdUtil;
import org.whispersystems.textsecuregcm.storage.KeysManager;

class KeysGrpcHelper {

  static final byte ALL_DEVICES = 0;

  /// Fetch {@link AccountPreKeyBundles} from the targetAccount
  ///
  /// @param targetAccount the account to fetch pre-key bundles from
  /// @param targetServiceIdentifier the service identifier for the target Account
  /// @param targetDeviceId the device ID to retrieve pre-key bundles for, or [#ALL_DEVICES] if all devices should be
  /// retrieved
  /// @param keysManager The {@link KeysManager} to lookup pre-keys from
  ///
  /// @return the requested bundles, or empty if the keys for the `targetAccount` do not exist
  static Optional<AccountPreKeyBundles> getPreKeys(final Account targetAccount,
      final ServiceIdentifier targetServiceIdentifier,
      final byte targetDeviceId,
      final KeysManager keysManager) {

    final Stream<Device> devices = targetDeviceId == ALL_DEVICES
        ? targetAccount.getDevices().stream()
        : targetAccount.getDevice(targetDeviceId).stream();

    final String userAgent = RequestAttributesUtil.getUserAgent().orElse(null);

    final Map<Byte, CompletableFuture<Optional<KeysManager.DevicePreKeys>>> takeKeyFuturesByDeviceId =
        devices.collect(Collectors.toMap(
            Device::getId,
            device -> keysManager.takeDevicePreKeys(device.getId(), targetServiceIdentifier, userAgent)));

    CompletableFuture.allOf(takeKeyFuturesByDeviceId.values().toArray(CompletableFuture[]::new)).join();

    final Map<Byte, KeysManager.DevicePreKeys> preKeysByDeviceId = takeKeyFuturesByDeviceId.entrySet().stream()
        .filter(entry -> entry.getValue().resultNow().isPresent())
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().resultNow().orElseThrow()));

    if (preKeysByDeviceId.isEmpty()) {
      // If there were no devices with valid prekey bundles in the account, the account is gone
      return Optional.empty();
    }

    final AccountPreKeyBundles.Builder preKeyBundlesBuilder = AccountPreKeyBundles.newBuilder()
        .setIdentityKey(ByteString.copyFrom(targetAccount.getIdentityKey(targetServiceIdentifier.identityType()).serialize()))
        .setUnrestrictedUnidentifiedAccess(targetServiceIdentifier.identityType() == IdentityType.ACI && targetAccount.isUnrestrictedUnidentifiedAccess());

    // Set the UAK hash if the account has a UAK and we're targeting an ACI. PNIs don't allow unidentified access.
    targetAccount
        .getUnidentifiedAccessKey()
        .filter(_ -> targetServiceIdentifier.identityType() == IdentityType.ACI)
        .map(UnidentifiedAccessChecksum::generateFor)
        .map(ByteString::copyFrom)
        .ifPresent(preKeyBundlesBuilder::setUnidentifiedAccessKeyFingerprint);

    preKeysByDeviceId.forEach((deviceId, devicePreKeys) -> {
      final Device device = targetAccount.getDevice(deviceId).orElseThrow();


      final DevicePreKeyBundle.Builder builder = DevicePreKeyBundle.newBuilder()
          .setEcSignedPreKey(EcSignedPreKey.newBuilder()
              .setKeyId(KeyIdUtil.toUnsignedInt(devicePreKeys.ecSignedPreKey().keyId()))
              .setPublicKey(ByteString.copyFrom(devicePreKeys.ecSignedPreKey().serializedPublicKey()))
              .setSignature(ByteString.copyFrom(devicePreKeys.ecSignedPreKey().signature())))
          .setKemOneTimePreKey(KemSignedPreKey.newBuilder()
              .setKeyId(KeyIdUtil.toUnsignedInt(devicePreKeys.kemSignedPreKey().keyId()))
              .setPublicKey(ByteString.copyFrom(devicePreKeys.kemSignedPreKey().serializedPublicKey()))
              .setSignature(ByteString.copyFrom(devicePreKeys.kemSignedPreKey().signature())))
          .setRegistrationId(device.getRegistrationId(targetServiceIdentifier.identityType()));

      devicePreKeys.ecPreKey().ifPresent(ecPreKey -> builder.setEcOneTimePreKey(EcPreKey.newBuilder()
          .setKeyId(KeyIdUtil.toUnsignedInt(ecPreKey.keyId()))
          .setPublicKey(ByteString.copyFrom(ecPreKey.serializedPublicKey()))));

      preKeyBundlesBuilder.putDevicePreKeys(deviceId, builder.build());
    });

    return Optional.of(preKeyBundlesBuilder.build());
  }

  public static ECPreKey checkEcPreKey(final EcPreKey preKey, final StatusRuntimeException invalidPublicKeyException) {
    try {
      return new ECPreKey(preKey.getKeyId(), new ECPublicKey(preKey.getPublicKey().toByteArray()));
    } catch (final InvalidKeyException e) {
      throw invalidPublicKeyException;
    }
  }

  public static ECSignedPreKey checkEcSignedPreKey(final EcSignedPreKey preKey, final IdentityKey identityKey,
      final StatusRuntimeException invalidPublicKeyException, final StatusRuntimeException invalidSignatureException) {
    try {
      final ECSignedPreKey ecSignedPreKey = new ECSignedPreKey(preKey.getKeyId(),
          new ECPublicKey(preKey.getPublicKey().toByteArray()),
          preKey.getSignature().toByteArray());

      if (ecSignedPreKey.signatureValid(identityKey)) {
        return ecSignedPreKey;
      } else {
        throw invalidSignatureException;
      }
    } catch (final InvalidKeyException e) {
      throw invalidPublicKeyException;
    }
  }

  public static KEMSignedPreKey checkKemSignedPreKey(final KemSignedPreKey preKey, final IdentityKey identityKey,
      final StatusRuntimeException invalidPublicKeyException, final StatusRuntimeException invalidSignatureException) {
    try {
      final KEMSignedPreKey kemSignedPreKey = new KEMSignedPreKey(preKey.getKeyId(),
          new KEMPublicKey(preKey.getPublicKey().toByteArray()),
          preKey.getSignature().toByteArray());

      if (kemSignedPreKey.signatureValid(identityKey)) {
        return kemSignedPreKey;
      } else {
        throw invalidSignatureException;
      }
    } catch (final InvalidKeyException e) {
      throw invalidPublicKeyException;
    }
  }
}
