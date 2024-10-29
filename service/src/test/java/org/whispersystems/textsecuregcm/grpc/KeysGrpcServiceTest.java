/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertRateLimitExceeded;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusException;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.signal.chat.common.EcPreKey;
import org.signal.chat.common.EcSignedPreKey;
import org.signal.chat.common.KemSignedPreKey;
import org.signal.chat.common.ServiceIdentifier;
import org.signal.chat.keys.GetPreKeyCountRequest;
import org.signal.chat.keys.GetPreKeyCountResponse;
import org.signal.chat.keys.GetPreKeysRequest;
import org.signal.chat.keys.GetPreKeysResponse;
import org.signal.chat.keys.KeysGrpc;
import org.signal.chat.keys.SetEcSignedPreKeyRequest;
import org.signal.chat.keys.SetKemLastResortPreKeyRequest;
import org.signal.chat.keys.SetOneTimeEcPreKeysRequest;
import org.signal.chat.keys.SetOneTimeKemSignedPreKeysRequest;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.core.publisher.Mono;

class KeysGrpcServiceTest extends SimpleBaseGrpcTest<KeysGrpcService, KeysGrpc.KeysBlockingStub> {

  private static final ECKeyPair ACI_IDENTITY_KEY_PAIR = Curve.generateKeyPair();

  private static final ECKeyPair PNI_IDENTITY_KEY_PAIR = Curve.generateKeyPair();

  protected static final UUID AUTHENTICATED_PNI = UUID.randomUUID();

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private KeysManager keysManager;

  @Mock
  private RateLimiter preKeysRateLimiter;

  @Mock
  private Device authenticatedDevice;


  @Override
  protected KeysGrpcService createServiceBeforeEachTest() {
    final RateLimiters rateLimiters = mock(RateLimiters.class);
    when(rateLimiters.getPreKeysLimiter()).thenReturn(preKeysRateLimiter);

    when(preKeysRateLimiter.validateReactive(anyString())).thenReturn(Mono.empty());

    when(authenticatedDevice.getId()).thenReturn(AUTHENTICATED_DEVICE_ID);

    final Account authenticatedAccount = mock(Account.class);
    when(authenticatedAccount.getUuid()).thenReturn(AUTHENTICATED_ACI);
    when(authenticatedAccount.getPhoneNumberIdentifier()).thenReturn(AUTHENTICATED_PNI);
    when(authenticatedAccount.getIdentifier(IdentityType.ACI)).thenReturn(AUTHENTICATED_ACI);
    when(authenticatedAccount.getIdentifier(IdentityType.PNI)).thenReturn(AUTHENTICATED_PNI);
    when(authenticatedAccount.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(ACI_IDENTITY_KEY_PAIR.getPublicKey()));
    when(authenticatedAccount.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(PNI_IDENTITY_KEY_PAIR.getPublicKey()));
    when(authenticatedAccount.getDevice(AUTHENTICATED_DEVICE_ID)).thenReturn(Optional.of(authenticatedDevice));

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI)).thenReturn(Optional.of(authenticatedAccount));
    when(accountsManager.getByPhoneNumberIdentifier(AUTHENTICATED_PNI)).thenReturn(Optional.of(authenticatedAccount));

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI)).thenReturn(CompletableFuture.completedFuture(Optional.of(authenticatedAccount)));
    when(accountsManager.getByPhoneNumberIdentifierAsync(AUTHENTICATED_PNI)).thenReturn(CompletableFuture.completedFuture(Optional.of(authenticatedAccount)));

    return new KeysGrpcService(accountsManager, keysManager, rateLimiters);
  }

  @Test
  void getPreKeyCount() {
    when(keysManager.getEcCount(AUTHENTICATED_ACI, AUTHENTICATED_DEVICE_ID))
        .thenReturn(CompletableFuture.completedFuture(1));

    when(keysManager.getPqCount(AUTHENTICATED_ACI, AUTHENTICATED_DEVICE_ID))
        .thenReturn(CompletableFuture.completedFuture(2));

    when(keysManager.getEcCount(AUTHENTICATED_PNI, AUTHENTICATED_DEVICE_ID))
        .thenReturn(CompletableFuture.completedFuture(3));

    when(keysManager.getPqCount(AUTHENTICATED_PNI, AUTHENTICATED_DEVICE_ID))
        .thenReturn(CompletableFuture.completedFuture(4));

    assertEquals(GetPreKeyCountResponse.newBuilder()
            .setAciEcPreKeyCount(1)
            .setAciKemPreKeyCount(2)
            .setPniEcPreKeyCount(3)
            .setPniKemPreKeyCount(4)
            .build(),
        authenticatedServiceStub().getPreKeyCount(GetPreKeyCountRequest.newBuilder().build()));
  }

  @ParameterizedTest
  @EnumSource(value = org.signal.chat.common.IdentityType.class, names = {"IDENTITY_TYPE_ACI", "IDENTITY_TYPE_PNI"})
  void setOneTimeEcPreKeys(final org.signal.chat.common.IdentityType identityType) {
    final List<ECPreKey> preKeys = new ArrayList<>();

    for (int keyId = 0; keyId < 100; keyId++) {
      preKeys.add(new ECPreKey(keyId, Curve.generateKeyPair().getPublicKey()));
    }

    when(keysManager.storeEcOneTimePreKeys(any(), anyByte(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    //noinspection ResultOfMethodCallIgnored
    authenticatedServiceStub().setOneTimeEcPreKeys(SetOneTimeEcPreKeysRequest.newBuilder()
        .setIdentityType(identityType)
        .addAllPreKeys(preKeys.stream()
            .map(preKey -> EcPreKey.newBuilder()
                .setKeyId(preKey.keyId())
                .setPublicKey(ByteString.copyFrom(preKey.serializedPublicKey()))
                .build())
            .toList())
        .build());

    final UUID expectedIdentifier = switch (IdentityTypeUtil.fromGrpcIdentityType(identityType)) {
      case ACI -> AUTHENTICATED_ACI;
      case PNI -> AUTHENTICATED_PNI;
    };

    verify(keysManager).storeEcOneTimePreKeys(expectedIdentifier, AUTHENTICATED_DEVICE_ID, preKeys);
  }

  @ParameterizedTest
  @MethodSource
  void setOneTimeEcPreKeysWithError(final SetOneTimeEcPreKeysRequest request) {
    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().setOneTimeEcPreKeys(request));
  }

  private static Stream<Arguments> setOneTimeEcPreKeysWithError() {
    final SetOneTimeEcPreKeysRequest prototypeRequest = SetOneTimeEcPreKeysRequest.newBuilder()
        .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
        .addPreKeys(EcPreKey.newBuilder()
            .setKeyId(1)
            .setPublicKey(ByteString.copyFrom(Curve.generateKeyPair().getPublicKey().serialize()))
            .build())
        .build();

    return Stream.of(
        // Missing identity type
        Arguments.of(SetOneTimeEcPreKeysRequest.newBuilder(prototypeRequest)
            .clearIdentityType()
            .build()),

        // Invalid public key
        Arguments.of(SetOneTimeEcPreKeysRequest.newBuilder(prototypeRequest)
            .setPreKeys(0, EcPreKey.newBuilder(prototypeRequest.getPreKeys(0))
                .clearPublicKey()
                .build())
            .build()),

        // No keys
        Arguments.of(SetOneTimeEcPreKeysRequest.newBuilder(prototypeRequest)
            .clearPreKeys()
            .build())
    );
  }

  @ParameterizedTest
  @EnumSource(value = org.signal.chat.common.IdentityType.class, names = {"IDENTITY_TYPE_ACI", "IDENTITY_TYPE_PNI"})
  void setOneTimeKemSignedPreKeys(final org.signal.chat.common.IdentityType identityType) {
    final ECKeyPair identityKeyPair = switch (IdentityTypeUtil.fromGrpcIdentityType(identityType)) {
      case ACI -> ACI_IDENTITY_KEY_PAIR;
      case PNI -> PNI_IDENTITY_KEY_PAIR;
    };

    final List<KEMSignedPreKey> preKeys = new ArrayList<>();

    for (int keyId = 0; keyId < 100; keyId++) {
      preKeys.add(KeysHelper.signedKEMPreKey(keyId, identityKeyPair));
    }

    when(keysManager.storeKemOneTimePreKeys(any(), anyByte(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    //noinspection ResultOfMethodCallIgnored
    authenticatedServiceStub().setOneTimeKemSignedPreKeys(
        SetOneTimeKemSignedPreKeysRequest.newBuilder()
            .setIdentityType(identityType)
            .addAllPreKeys(preKeys.stream()
                .map(preKey -> KemSignedPreKey.newBuilder()
                    .setKeyId(preKey.keyId())
                    .setPublicKey(ByteString.copyFrom(preKey.serializedPublicKey()))
                    .setSignature(ByteString.copyFrom(preKey.signature()))
                    .build())
                .toList())
            .build());

    final UUID expectedIdentifier = switch (IdentityTypeUtil.fromGrpcIdentityType(identityType)) {
      case ACI -> AUTHENTICATED_ACI;
      case PNI -> AUTHENTICATED_PNI;
    };

    verify(keysManager).storeKemOneTimePreKeys(expectedIdentifier, AUTHENTICATED_DEVICE_ID, preKeys);
  }

  @ParameterizedTest
  @MethodSource
  void setOneTimeKemSignedPreKeysWithError(final SetOneTimeKemSignedPreKeysRequest request) {
    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().setOneTimeKemSignedPreKeys(request));
  }

  private static Stream<Arguments> setOneTimeKemSignedPreKeysWithError() {
    final KEMSignedPreKey signedPreKey = KeysHelper.signedKEMPreKey(1, ACI_IDENTITY_KEY_PAIR);

    final SetOneTimeKemSignedPreKeysRequest prototypeRequest = SetOneTimeKemSignedPreKeysRequest.newBuilder()
        .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
        .addPreKeys(KemSignedPreKey.newBuilder()
            .setKeyId(1)
            .setPublicKey(ByteString.copyFrom(signedPreKey.serializedPublicKey()))
            .setSignature(ByteString.copyFrom(signedPreKey.signature()))
            .build())
        .build();

    return Stream.of(
        // Missing identity type
        Arguments.of(SetOneTimeKemSignedPreKeysRequest.newBuilder(prototypeRequest)
            .clearIdentityType()
            .build()),

        // Invalid public key
        Arguments.of(SetOneTimeKemSignedPreKeysRequest.newBuilder(prototypeRequest)
            .setPreKeys(0, KemSignedPreKey.newBuilder(prototypeRequest.getPreKeys(0))
                .clearPublicKey()
                .build())
            .build()),

        // Invalid signature
        Arguments.of(SetOneTimeKemSignedPreKeysRequest.newBuilder(prototypeRequest)
            .setPreKeys(0, KemSignedPreKey.newBuilder(prototypeRequest.getPreKeys(0))
                .clearSignature()
                .build())
            .build()),

        // No keys
        Arguments.of(SetOneTimeKemSignedPreKeysRequest.newBuilder(prototypeRequest)
            .clearPreKeys()
            .build())
    );
  }

  @ParameterizedTest
  @EnumSource(value = org.signal.chat.common.IdentityType.class, names = {"IDENTITY_TYPE_ACI", "IDENTITY_TYPE_PNI"})
  void setSignedPreKey(final org.signal.chat.common.IdentityType identityType) {
    when(accountsManager.updateDeviceAsync(any(), anyByte(), any())).thenAnswer(invocation -> {
      final Account account = invocation.getArgument(0);
      final byte deviceId = invocation.getArgument(1);
      final Consumer<Device> deviceUpdater = invocation.getArgument(2);

      account.getDevice(deviceId).ifPresent(deviceUpdater);

      return CompletableFuture.completedFuture(account);
    });

    when(keysManager.storeEcSignedPreKeys(any(), anyByte(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final ECKeyPair identityKeyPair = switch (IdentityTypeUtil.fromGrpcIdentityType(identityType)) {
      case ACI -> ACI_IDENTITY_KEY_PAIR;
      case PNI -> PNI_IDENTITY_KEY_PAIR;
    };

    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(17, identityKeyPair);

    //noinspection ResultOfMethodCallIgnored
    authenticatedServiceStub().setEcSignedPreKey(SetEcSignedPreKeyRequest.newBuilder()
            .setIdentityType(identityType)
            .setSignedPreKey(EcSignedPreKey.newBuilder()
                .setKeyId(signedPreKey.keyId())
                .setPublicKey(ByteString.copyFrom(signedPreKey.serializedPublicKey()))
                .setSignature(ByteString.copyFrom(signedPreKey.signature()))
                .build())
            .build());

    final UUID expectedIdentifier = switch (identityType) {
      case IDENTITY_TYPE_ACI -> AUTHENTICATED_ACI;
      case IDENTITY_TYPE_PNI -> AUTHENTICATED_PNI;
      default -> throw new IllegalArgumentException("Unexpected identity type");
    };

    verify(keysManager).storeEcSignedPreKeys(expectedIdentifier, AUTHENTICATED_DEVICE_ID, signedPreKey);
  }

  @ParameterizedTest
  @MethodSource
  void setSignedPreKeyWithError(final SetEcSignedPreKeyRequest request) {
    final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class, () -> authenticatedServiceStub().setEcSignedPreKey(request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
  }

  private static Stream<Arguments> setSignedPreKeyWithError() {
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(17, ACI_IDENTITY_KEY_PAIR);

    final SetEcSignedPreKeyRequest prototypeRequest = SetEcSignedPreKeyRequest.newBuilder()
        .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
        .setSignedPreKey(EcSignedPreKey.newBuilder()
            .setKeyId(signedPreKey.keyId())
            .setPublicKey(ByteString.copyFrom(signedPreKey.serializedPublicKey()))
            .setSignature(ByteString.copyFrom(signedPreKey.signature()))
            .build())
        .build();

    return Stream.of(
        // Missing identity type
        Arguments.of(SetEcSignedPreKeyRequest.newBuilder(prototypeRequest)
            .clearIdentityType()
            .build()),

        // Invalid public key
        Arguments.of(SetEcSignedPreKeyRequest.newBuilder(prototypeRequest)
                .setSignedPreKey(EcSignedPreKey.newBuilder(prototypeRequest.getSignedPreKey())
                    .clearPublicKey()
                    .build())
                .build()),

        // Invalid signature
        Arguments.of(SetEcSignedPreKeyRequest.newBuilder(prototypeRequest)
            .setSignedPreKey(EcSignedPreKey.newBuilder(prototypeRequest.getSignedPreKey())
                .clearSignature()
                .build())
            .build()),

        // Missing key
        Arguments.of(SetEcSignedPreKeyRequest.newBuilder(prototypeRequest)
            .clearSignedPreKey()
            .build())
    );
  }

  @ParameterizedTest
  @EnumSource(value = org.signal.chat.common.IdentityType.class, names = {"IDENTITY_TYPE_ACI", "IDENTITY_TYPE_PNI"})
  void setLastResortPreKey(final org.signal.chat.common.IdentityType identityType) {
    when(keysManager.storePqLastResort(any(), anyByte(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final ECKeyPair identityKeyPair = switch (IdentityTypeUtil.fromGrpcIdentityType(identityType)) {
      case ACI -> ACI_IDENTITY_KEY_PAIR;
      case PNI -> PNI_IDENTITY_KEY_PAIR;
    };

    final KEMSignedPreKey lastResortPreKey = KeysHelper.signedKEMPreKey(17, identityKeyPair);

    //noinspection ResultOfMethodCallIgnored
    authenticatedServiceStub().setKemLastResortPreKey(SetKemLastResortPreKeyRequest.newBuilder()
            .setIdentityType(identityType)
            .setSignedPreKey(KemSignedPreKey.newBuilder()
                .setKeyId(lastResortPreKey.keyId())
                .setPublicKey(ByteString.copyFrom(lastResortPreKey.serializedPublicKey()))
                .setSignature(ByteString.copyFrom(lastResortPreKey.signature()))
                .build())
            .build());

    final UUID expectedIdentifier = switch (identityType) {
      case IDENTITY_TYPE_ACI -> AUTHENTICATED_ACI;
      case IDENTITY_TYPE_PNI -> AUTHENTICATED_PNI;
      case IDENTITY_TYPE_UNSPECIFIED, UNRECOGNIZED -> throw new AssertionError("Bad identity type");
    };

    verify(keysManager).storePqLastResort(expectedIdentifier, AUTHENTICATED_DEVICE_ID, lastResortPreKey);
  }

  @ParameterizedTest
  @MethodSource
  void setLastResortPreKeyWithError(final SetKemLastResortPreKeyRequest request) {
    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().setKemLastResortPreKey(request));
  }

  private static Stream<Arguments> setLastResortPreKeyWithError() {
    final KEMSignedPreKey lastResortPreKey = KeysHelper.signedKEMPreKey(17, ACI_IDENTITY_KEY_PAIR);

    final SetKemLastResortPreKeyRequest prototypeRequest = SetKemLastResortPreKeyRequest.newBuilder()
        .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
        .setSignedPreKey(KemSignedPreKey.newBuilder()
            .setKeyId(lastResortPreKey.keyId())
            .setPublicKey(ByteString.copyFrom(lastResortPreKey.serializedPublicKey()))
            .setSignature(ByteString.copyFrom(lastResortPreKey.signature()))
            .build())
        .build();

    return Stream.of(
        // No identity type
        Arguments.of(SetKemLastResortPreKeyRequest.newBuilder(prototypeRequest)
            .clearIdentityType()
            .build()),

        // Bad public key
        Arguments.of(SetKemLastResortPreKeyRequest.newBuilder(prototypeRequest)
            .setSignedPreKey(KemSignedPreKey.newBuilder(prototypeRequest.getSignedPreKey())
                .clearPublicKey()
                .build())
            .build()),

        // Bad signature
        Arguments.of(SetKemLastResortPreKeyRequest.newBuilder(prototypeRequest)
            .setSignedPreKey(KemSignedPreKey.newBuilder(prototypeRequest.getSignedPreKey())
                .clearSignature()
                .build())
            .build()),

        // Missing key
        Arguments.of(SetKemLastResortPreKeyRequest.newBuilder(prototypeRequest)
            .clearSignedPreKey()
            .build())
    );
  }

  @ParameterizedTest
  @EnumSource(value = org.signal.chat.common.IdentityType.class, names = {"IDENTITY_TYPE_ACI", "IDENTITY_TYPE_PNI"})
  void getPreKeys(final org.signal.chat.common.IdentityType grpcIdentityType) {
    final Account targetAccount = mock(Account.class);

    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());
    final UUID identifier = UUID.randomUUID();

    final IdentityType identityType = IdentityTypeUtil.fromGrpcIdentityType(grpcIdentityType);

    when(targetAccount.getUuid()).thenReturn(UUID.randomUUID());
    when(targetAccount.getIdentifier(identityType)).thenReturn(identifier);
    when(targetAccount.getIdentityKey(identityType)).thenReturn(identityKey);
    when(accountsManager.getByServiceIdentifierAsync(argThat(serviceIdentifier -> serviceIdentifier.uuid().equals(identifier))))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    final Map<Byte, ECPreKey> ecOneTimePreKeys = new HashMap<>();
    final Map<Byte, KEMSignedPreKey> kemPreKeys = new HashMap<>();
    final Map<Byte, ECSignedPreKey> ecSignedPreKeys = new HashMap<>();

    final Map<Byte, Device> devices = new HashMap<>();

    final byte deviceId1 = 1;
    final byte deviceId2 = 2;

    for (final byte deviceId : List.of(deviceId1, deviceId2)) {
      ecOneTimePreKeys.put(deviceId, new ECPreKey(1, Curve.generateKeyPair().getPublicKey()));
      kemPreKeys.put(deviceId, KeysHelper.signedKEMPreKey(2, identityKeyPair));
      ecSignedPreKeys.put(deviceId, KeysHelper.signedECPreKey(3, identityKeyPair));

      final Device device = mock(Device.class);
      when(device.getId()).thenReturn(deviceId);

      devices.put(deviceId, device);
      when(targetAccount.getDevice(deviceId)).thenReturn(Optional.of(device));
    }

    when(targetAccount.getDevices()).thenReturn(new ArrayList<>(devices.values()));

    ecOneTimePreKeys.forEach((deviceId, preKey) -> when(keysManager.takeEC(identifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(preKey))));

    ecSignedPreKeys.forEach((deviceId, preKey) -> when(keysManager.getEcSignedPreKey(identifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(preKey))));

    kemPreKeys.forEach((deviceId, preKey) -> when(keysManager.takePQ(identifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(preKey))));

    {
      final GetPreKeysResponse response = authenticatedServiceStub().getPreKeys(GetPreKeysRequest.newBuilder()
          .setTargetIdentifier(ServiceIdentifier.newBuilder()
              .setIdentityType(grpcIdentityType)
              .setUuid(UUIDUtil.toByteString(identifier))
              .build())
          .setDeviceId(1)
          .build());

      final GetPreKeysResponse expectedResponse = GetPreKeysResponse.newBuilder()
          .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
          .putPreKeys(1, GetPreKeysResponse.PreKeyBundle.newBuilder()
              .setEcSignedPreKey(EcSignedPreKey.newBuilder()
                  .setKeyId(ecSignedPreKeys.get(deviceId1).keyId())
                  .setPublicKey(ByteString.copyFrom(ecSignedPreKeys.get(deviceId1).serializedPublicKey()))
                  .setSignature(ByteString.copyFrom(ecSignedPreKeys.get(deviceId1).signature()))
                  .build())
              .setEcOneTimePreKey(EcPreKey.newBuilder()
                  .setKeyId(ecOneTimePreKeys.get(deviceId1).keyId())
                  .setPublicKey(ByteString.copyFrom(ecOneTimePreKeys.get(deviceId1).serializedPublicKey()))
                  .build())
              .setKemOneTimePreKey(KemSignedPreKey.newBuilder()
                  .setKeyId(kemPreKeys.get(deviceId1).keyId())
                  .setPublicKey(ByteString.copyFrom(kemPreKeys.get(deviceId1).serializedPublicKey()))
                  .setSignature(ByteString.copyFrom(kemPreKeys.get(deviceId1).signature()))
                  .build())
              .build())
          .build();

      assertEquals(expectedResponse, response);
    }

    when(keysManager.takeEC(identifier, deviceId2)).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    when(keysManager.takePQ(identifier, deviceId2)).thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    {
      final GetPreKeysResponse response = authenticatedServiceStub().getPreKeys(GetPreKeysRequest.newBuilder()
          .setTargetIdentifier(ServiceIdentifier.newBuilder()
              .setIdentityType(grpcIdentityType)
              .setUuid(UUIDUtil.toByteString(identifier))
              .build())
          .build());

      final GetPreKeysResponse expectedResponse = GetPreKeysResponse.newBuilder()
          .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
          .putPreKeys(1, GetPreKeysResponse.PreKeyBundle.newBuilder()
              .setEcSignedPreKey(EcSignedPreKey.newBuilder()
                  .setKeyId(ecSignedPreKeys.get(deviceId1).keyId())
                  .setPublicKey(ByteString.copyFrom(ecSignedPreKeys.get(deviceId1).serializedPublicKey()))
                  .setSignature(ByteString.copyFrom(ecSignedPreKeys.get(deviceId1).signature()))
                  .build())
              .setEcOneTimePreKey(EcPreKey.newBuilder()
                  .setKeyId(ecOneTimePreKeys.get(deviceId1).keyId())
                  .setPublicKey(ByteString.copyFrom(ecOneTimePreKeys.get(deviceId1).serializedPublicKey()))
                  .build())
              .setKemOneTimePreKey(KemSignedPreKey.newBuilder()
                  .setKeyId(kemPreKeys.get(deviceId1).keyId())
                  .setPublicKey(ByteString.copyFrom(kemPreKeys.get(deviceId1).serializedPublicKey()))
                  .setSignature(ByteString.copyFrom(kemPreKeys.get(deviceId1).signature()))
                  .build())
              .build())
          .putPreKeys(2, GetPreKeysResponse.PreKeyBundle.newBuilder()
              .setEcSignedPreKey(EcSignedPreKey.newBuilder()
                  .setKeyId(ecSignedPreKeys.get(deviceId2).keyId())
                  .setPublicKey(ByteString.copyFrom(ecSignedPreKeys.get(deviceId2).serializedPublicKey()))
                  .setSignature(ByteString.copyFrom(ecSignedPreKeys.get(deviceId2).signature()))
                  .build())
              .build())
          .build();

      assertEquals(expectedResponse, response);
    }
  }

  @Test
  void getPreKeysAccountNotFound() {
    when(accountsManager.getByServiceIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    assertStatusException(Status.NOT_FOUND, () -> authenticatedServiceStub().getPreKeys(GetPreKeysRequest.newBuilder()
        .setTargetIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(UUIDUtil.toByteString(UUID.randomUUID()))
            .build())
        .build()));
  }

  @Test
  void getPreKeysDeviceNotFound() {
    final UUID accountIdentifier = UUID.randomUUID();

    final Account targetAccount = mock(Account.class);
    when(targetAccount.getUuid()).thenReturn(accountIdentifier);
    when(targetAccount.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(Curve.generateKeyPair().getPublicKey()));
    when(targetAccount.getDevices()).thenReturn(Collections.emptyList());
    when(targetAccount.getDevice(anyByte())).thenReturn(Optional.empty());

    when(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(accountIdentifier)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    assertStatusException(Status.NOT_FOUND, () -> authenticatedServiceStub().getPreKeys(GetPreKeysRequest.newBuilder()
        .setTargetIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(UUIDUtil.toByteString(accountIdentifier))
            .build())
        .setDeviceId(Device.PRIMARY_ID)
        .build()));
  }

  @Test
  void getPreKeysRateLimited() {
    final Account targetAccount = mock(Account.class);
    when(targetAccount.getUuid()).thenReturn(UUID.randomUUID());
    when(targetAccount.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(Curve.generateKeyPair().getPublicKey()));
    when(targetAccount.getDevices()).thenReturn(Collections.emptyList());
    when(targetAccount.getDevice(anyByte())).thenReturn(Optional.empty());

    when(accountsManager.getByServiceIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    final Duration retryAfterDuration = Duration.ofMinutes(7);
    when(preKeysRateLimiter.validateReactive(anyString()))
        .thenReturn(Mono.error(new RateLimitExceededException(retryAfterDuration)));

    assertRateLimitExceeded(retryAfterDuration, () -> authenticatedServiceStub().getPreKeys(GetPreKeysRequest.newBuilder()
        .setTargetIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(UUIDUtil.toByteString(UUID.randomUUID()))
            .build())
        .build()));
    verifyNoInteractions(accountsManager);
  }
}
