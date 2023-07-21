/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.ServerInterceptors;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
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
import org.whispersystems.textsecuregcm.auth.grpc.MockAuthenticationInterceptor;
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

class KeysGrpcServiceTest {

  private AccountsManager accountsManager;
  private KeysManager keysManager;
  private RateLimiter preKeysRateLimiter;

  private Device authenticatedDevice;

  private KeysGrpc.KeysBlockingStub keysStub;

  private static final UUID AUTHENTICATED_ACI = UUID.randomUUID();
  private static final UUID AUTHENTICATED_PNI = UUID.randomUUID();
  private static final long AUTHENTICATED_DEVICE_ID = Device.MASTER_ID;

  private static final ECKeyPair ACI_IDENTITY_KEY_PAIR = Curve.generateKeyPair();
  private static final ECKeyPair PNI_IDENTITY_KEY_PAIR = Curve.generateKeyPair();

  @RegisterExtension
  static final GrpcServerExtension GRPC_SERVER_EXTENSION = new GrpcServerExtension();

  @BeforeEach
  void setUp() {
    accountsManager = mock(AccountsManager.class);
    keysManager = mock(KeysManager.class);
    preKeysRateLimiter = mock(RateLimiter.class);

    final RateLimiters rateLimiters = mock(RateLimiters.class);
    when(rateLimiters.getPreKeysLimiter()).thenReturn(preKeysRateLimiter);

    when(preKeysRateLimiter.validateReactive(anyString())).thenReturn(Mono.empty());

    final KeysGrpcService keysGrpcService = new KeysGrpcService(accountsManager, keysManager, rateLimiters);
    keysStub = KeysGrpc.newBlockingStub(GRPC_SERVER_EXTENSION.getChannel());

    authenticatedDevice = mock(Device.class);
    when(authenticatedDevice.getId()).thenReturn(AUTHENTICATED_DEVICE_ID);

    final Account authenticatedAccount = mock(Account.class);
    when(authenticatedAccount.getUuid()).thenReturn(AUTHENTICATED_ACI);
    when(authenticatedAccount.getPhoneNumberIdentifier()).thenReturn(AUTHENTICATED_PNI);
    when(authenticatedAccount.getIdentifier(IdentityType.ACI)).thenReturn(AUTHENTICATED_ACI);
    when(authenticatedAccount.getIdentifier(IdentityType.PNI)).thenReturn(AUTHENTICATED_PNI);
    when(authenticatedAccount.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(ACI_IDENTITY_KEY_PAIR.getPublicKey()));
    when(authenticatedAccount.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(PNI_IDENTITY_KEY_PAIR.getPublicKey()));
    when(authenticatedAccount.getDevice(AUTHENTICATED_DEVICE_ID)).thenReturn(Optional.of(authenticatedDevice));

    final MockAuthenticationInterceptor mockAuthenticationInterceptor = new MockAuthenticationInterceptor();
    mockAuthenticationInterceptor.setAuthenticatedDevice(AUTHENTICATED_ACI, AUTHENTICATED_DEVICE_ID);

    GRPC_SERVER_EXTENSION.getServiceRegistry()
        .addService(ServerInterceptors.intercept(keysGrpcService, mockAuthenticationInterceptor));

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI)).thenReturn(Optional.of(authenticatedAccount));
    when(accountsManager.getByPhoneNumberIdentifier(AUTHENTICATED_PNI)).thenReturn(Optional.of(authenticatedAccount));

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI)).thenReturn(CompletableFuture.completedFuture(Optional.of(authenticatedAccount)));
    when(accountsManager.getByPhoneNumberIdentifierAsync(AUTHENTICATED_PNI)).thenReturn(CompletableFuture.completedFuture(Optional.of(authenticatedAccount)));
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
        keysStub.getPreKeyCount(GetPreKeyCountRequest.newBuilder().build()));
  }

  @ParameterizedTest
  @EnumSource(value = org.signal.chat.common.IdentityType.class, names = {"IDENTITY_TYPE_ACI", "IDENTITY_TYPE_PNI"})
  void setOneTimeEcPreKeys(final org.signal.chat.common.IdentityType identityType) {
    final List<ECPreKey> preKeys = new ArrayList<>();

    for (int keyId = 0; keyId < 100; keyId++) {
      preKeys.add(new ECPreKey(keyId, Curve.generateKeyPair().getPublicKey()));
    }

    when(keysManager.storeEcOneTimePreKeys(any(), anyLong(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    //noinspection ResultOfMethodCallIgnored
    keysStub.setOneTimeEcPreKeys(SetOneTimeEcPreKeysRequest.newBuilder()
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
    @SuppressWarnings("ResultOfMethodCallIgnored") final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class, () -> keysStub.setOneTimeEcPreKeys(request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
  }

  private static Stream<Arguments> setOneTimeEcPreKeysWithError() {
    return Stream.of(
        // Missing identity type
        Arguments.of(SetOneTimeEcPreKeysRequest.newBuilder()
            .addPreKeys(EcPreKey.newBuilder()
                .setKeyId(1)
                .setPublicKey(ByteString.copyFrom(Curve.generateKeyPair().getPublicKey().serialize()))
                .build())
            .build()),

        // Invalid public key
        Arguments.of(SetOneTimeEcPreKeysRequest.newBuilder()
            .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
            .addPreKeys(EcPreKey.newBuilder()
                .setKeyId(1)
                .setPublicKey(ByteString.empty())
                .build())
            .build()),

        // No keys
        Arguments.of(SetOneTimeEcPreKeysRequest.newBuilder()
            .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
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

    when(keysManager.storeKemOneTimePreKeys(any(), anyLong(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    //noinspection ResultOfMethodCallIgnored
    keysStub.setOneTimeKemSignedPreKeys(
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
    @SuppressWarnings("ResultOfMethodCallIgnored") final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class, () -> keysStub.setOneTimeKemSignedPreKeys(request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
  }

  private static Stream<Arguments> setOneTimeKemSignedPreKeysWithError() {
    final KEMSignedPreKey signedPreKey = KeysHelper.signedKEMPreKey(1, ACI_IDENTITY_KEY_PAIR);

    return Stream.of(
        // Missing identity type
        Arguments.of(SetOneTimeKemSignedPreKeysRequest.newBuilder()
            .addPreKeys(KemSignedPreKey.newBuilder()
                .setKeyId(1)
                .setPublicKey(ByteString.copyFrom(signedPreKey.serializedPublicKey()))
                .setSignature(ByteString.copyFrom(signedPreKey.signature()))
                .build())
            .build()),

        // Invalid public key
        Arguments.of(SetOneTimeKemSignedPreKeysRequest.newBuilder()
            .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
            .addPreKeys(KemSignedPreKey.newBuilder()
                .setKeyId(1)
                .setPublicKey(ByteString.empty())
                .setSignature(ByteString.copyFrom(signedPreKey.signature()))
                .build())
            .build()),

        // Invalid signature
        Arguments.of(SetOneTimeKemSignedPreKeysRequest.newBuilder()
            .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
            .addPreKeys(KemSignedPreKey.newBuilder()
                .setKeyId(1)
                .setPublicKey(ByteString.copyFrom(signedPreKey.serializedPublicKey()))
                .setSignature(ByteString.empty())
                .build())
            .build()),

        // No keys
        Arguments.of(SetOneTimeKemSignedPreKeysRequest.newBuilder()
            .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
            .build())
    );
  }

  @ParameterizedTest
  @EnumSource(value = org.signal.chat.common.IdentityType.class, names = {"IDENTITY_TYPE_ACI", "IDENTITY_TYPE_PNI"})
  void setSignedPreKey(final org.signal.chat.common.IdentityType identityType) {
    when(accountsManager.updateDeviceAsync(any(), anyLong(), any())).thenAnswer(invocation -> {
      final Account account = invocation.getArgument(0);
      final long deviceId = invocation.getArgument(1);
      final Consumer<Device> deviceUpdater = invocation.getArgument(2);

      account.getDevice(deviceId).ifPresent(deviceUpdater);

      return CompletableFuture.completedFuture(account);
    });

    when(keysManager.storeEcSignedPreKeys(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final ECKeyPair identityKeyPair = switch (IdentityTypeUtil.fromGrpcIdentityType(identityType)) {
      case ACI -> ACI_IDENTITY_KEY_PAIR;
      case PNI -> PNI_IDENTITY_KEY_PAIR;
    };

    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(17, identityKeyPair);

    //noinspection ResultOfMethodCallIgnored
    keysStub.setEcSignedPreKey(SetEcSignedPreKeyRequest.newBuilder()
            .setIdentityType(identityType)
            .setSignedPreKey(EcSignedPreKey.newBuilder()
                .setKeyId(signedPreKey.keyId())
                .setPublicKey(ByteString.copyFrom(signedPreKey.serializedPublicKey()))
                .setSignature(ByteString.copyFrom(signedPreKey.signature()))
                .build())
            .build());

    switch (identityType) {
      case IDENTITY_TYPE_ACI -> {
        verify(authenticatedDevice).setSignedPreKey(signedPreKey);
        verify(keysManager).storeEcSignedPreKeys(AUTHENTICATED_ACI, Map.of(AUTHENTICATED_DEVICE_ID, signedPreKey));
      }

      case IDENTITY_TYPE_PNI -> {
        verify(authenticatedDevice).setPhoneNumberIdentitySignedPreKey(signedPreKey);
        verify(keysManager).storeEcSignedPreKeys(AUTHENTICATED_PNI, Map.of(AUTHENTICATED_DEVICE_ID, signedPreKey));
      }
    }
  }

  @ParameterizedTest
  @MethodSource
  void setSignedPreKeyWithError(final SetEcSignedPreKeyRequest request) {
    @SuppressWarnings("ResultOfMethodCallIgnored") final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class, () -> keysStub.setEcSignedPreKey(request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
  }

  private static Stream<Arguments> setSignedPreKeyWithError() {
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(17, ACI_IDENTITY_KEY_PAIR);

    return Stream.of(
        // Missing identity type
        Arguments.of(SetEcSignedPreKeyRequest.newBuilder()
            .setSignedPreKey(EcSignedPreKey.newBuilder()
                .setKeyId(signedPreKey.keyId())
                .setPublicKey(ByteString.copyFrom(signedPreKey.serializedPublicKey()))
                .setSignature(ByteString.copyFrom(signedPreKey.signature()))
                .build())
            .build()),

        // Invalid public key
        Arguments.of(SetEcSignedPreKeyRequest.newBuilder()
                .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
                .setSignedPreKey(EcSignedPreKey.newBuilder()
                    .setKeyId(signedPreKey.keyId())
                    .setPublicKey(ByteString.empty())
                    .setSignature(ByteString.copyFrom(signedPreKey.signature()))
                    .build())
                .build()),

        // Invalid signature
        Arguments.of(SetEcSignedPreKeyRequest.newBuilder()
                .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
                .setSignedPreKey(EcSignedPreKey.newBuilder()
                    .setKeyId(signedPreKey.keyId())
                    .setPublicKey(ByteString.copyFrom(signedPreKey.serializedPublicKey()))
                    .setSignature(ByteString.empty())
                    .build())
                .build()),

        // Missing key
        Arguments.of(SetEcSignedPreKeyRequest.newBuilder()
            .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
            .build())
    );
  }

  @ParameterizedTest
  @EnumSource(value = org.signal.chat.common.IdentityType.class, names = {"IDENTITY_TYPE_ACI", "IDENTITY_TYPE_PNI"})
  void setLastResortPreKey(final org.signal.chat.common.IdentityType identityType) {
    when(keysManager.storePqLastResort(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final ECKeyPair identityKeyPair = switch (IdentityTypeUtil.fromGrpcIdentityType(identityType)) {
      case ACI -> ACI_IDENTITY_KEY_PAIR;
      case PNI -> PNI_IDENTITY_KEY_PAIR;
    };

    final KEMSignedPreKey lastResortPreKey = KeysHelper.signedKEMPreKey(17, identityKeyPair);

    //noinspection ResultOfMethodCallIgnored
    keysStub.setKemLastResortPreKey(SetKemLastResortPreKeyRequest.newBuilder()
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

    verify(keysManager).storePqLastResort(expectedIdentifier, Map.of(AUTHENTICATED_DEVICE_ID, lastResortPreKey));
  }

  @ParameterizedTest
  @MethodSource
  void setLastResortPreKeyWithError(final SetKemLastResortPreKeyRequest request) {
    @SuppressWarnings("ResultOfMethodCallIgnored") final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class, () -> keysStub.setKemLastResortPreKey(request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
  }

  private static Stream<Arguments> setLastResortPreKeyWithError() {
    final KEMSignedPreKey lastResortPreKey = KeysHelper.signedKEMPreKey(17, ACI_IDENTITY_KEY_PAIR);

    return Stream.of(
        // No identity type
        Arguments.of(SetKemLastResortPreKeyRequest.newBuilder()
            .setSignedPreKey(KemSignedPreKey.newBuilder()
                .setKeyId(lastResortPreKey.keyId())
                .setPublicKey(ByteString.copyFrom(lastResortPreKey.serializedPublicKey()))
                .setSignature(ByteString.copyFrom(lastResortPreKey.signature()))
                .build())
            .build()),

        // Bad public key
        Arguments.of(SetKemLastResortPreKeyRequest.newBuilder()
            .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
            .setSignedPreKey(KemSignedPreKey.newBuilder()
                .setKeyId(lastResortPreKey.keyId())
                .setPublicKey(ByteString.empty())
                .setSignature(ByteString.copyFrom(lastResortPreKey.signature()))
                .build())
            .build()),

        // Bad signature
        Arguments.of(SetKemLastResortPreKeyRequest.newBuilder()
            .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
            .setSignedPreKey(KemSignedPreKey.newBuilder()
                .setKeyId(lastResortPreKey.keyId())
                .setPublicKey(ByteString.copyFrom(lastResortPreKey.serializedPublicKey()))
                .setSignature(ByteString.empty())
                .build())
            .build()),

        // Missing key
        Arguments.of(SetKemLastResortPreKeyRequest.newBuilder()
            .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
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

    final Map<Long, ECPreKey> ecOneTimePreKeys = new HashMap<>();
    final Map<Long, KEMSignedPreKey> kemPreKeys = new HashMap<>();
    final Map<Long, ECSignedPreKey> ecSignedPreKeys = new HashMap<>();

    final Map<Long, Device> devices = new HashMap<>();

    for (final long deviceId : List.of(1, 2)) {
      ecOneTimePreKeys.put(deviceId, new ECPreKey(1, Curve.generateKeyPair().getPublicKey()));
      kemPreKeys.put(deviceId, KeysHelper.signedKEMPreKey(2, identityKeyPair));
      ecSignedPreKeys.put(deviceId, KeysHelper.signedECPreKey(3, identityKeyPair));

      final Device device = mock(Device.class);
      when(device.getId()).thenReturn(deviceId);
      when(device.isEnabled()).thenReturn(true);
      when(device.getSignedPreKey(identityType)).thenReturn(ecSignedPreKeys.get(deviceId));

      devices.put(deviceId, device);
      when(targetAccount.getDevice(deviceId)).thenReturn(Optional.of(device));
    }

    when(targetAccount.getDevices()).thenReturn(new ArrayList<>(devices.values()));

    ecOneTimePreKeys.forEach((deviceId, preKey) -> when(keysManager.takeEC(identifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(preKey))));

    kemPreKeys.forEach((deviceId, preKey) -> when(keysManager.takePQ(identifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(preKey))));

    {
      final GetPreKeysResponse response = keysStub.getPreKeys(GetPreKeysRequest.newBuilder()
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
                  .setKeyId(ecSignedPreKeys.get(1L).keyId())
                  .setPublicKey(ByteString.copyFrom(ecSignedPreKeys.get(1L).serializedPublicKey()))
                  .setSignature(ByteString.copyFrom(ecSignedPreKeys.get(1L).signature()))
                  .build())
              .setEcOneTimePreKey(EcPreKey.newBuilder()
                  .setKeyId(ecOneTimePreKeys.get(1L).keyId())
                  .setPublicKey(ByteString.copyFrom(ecOneTimePreKeys.get(1L).serializedPublicKey()))
                  .build())
              .setKemOneTimePreKey(KemSignedPreKey.newBuilder()
                  .setKeyId(kemPreKeys.get(1L).keyId())
                  .setPublicKey(ByteString.copyFrom(kemPreKeys.get(1L).serializedPublicKey()))
                  .setSignature(ByteString.copyFrom(kemPreKeys.get(1L).signature()))
                  .build())
              .build())
          .build();

      assertEquals(expectedResponse, response);
    }

    when(keysManager.takeEC(identifier, 2)).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    when(keysManager.takePQ(identifier, 2)).thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    {
      final GetPreKeysResponse response = keysStub.getPreKeys(GetPreKeysRequest.newBuilder()
          .setTargetIdentifier(ServiceIdentifier.newBuilder()
              .setIdentityType(grpcIdentityType)
              .setUuid(UUIDUtil.toByteString(identifier))
              .build())
          .build());

      final GetPreKeysResponse expectedResponse = GetPreKeysResponse.newBuilder()
          .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
          .putPreKeys(1, GetPreKeysResponse.PreKeyBundle.newBuilder()
              .setEcSignedPreKey(EcSignedPreKey.newBuilder()
                  .setKeyId(ecSignedPreKeys.get(1L).keyId())
                  .setPublicKey(ByteString.copyFrom(ecSignedPreKeys.get(1L).serializedPublicKey()))
                  .setSignature(ByteString.copyFrom(ecSignedPreKeys.get(1L).signature()))
                  .build())
              .setEcOneTimePreKey(EcPreKey.newBuilder()
                  .setKeyId(ecOneTimePreKeys.get(1L).keyId())
                  .setPublicKey(ByteString.copyFrom(ecOneTimePreKeys.get(1L).serializedPublicKey()))
                  .build())
              .setKemOneTimePreKey(KemSignedPreKey.newBuilder()
                  .setKeyId(kemPreKeys.get(1L).keyId())
                  .setPublicKey(ByteString.copyFrom(kemPreKeys.get(1L).serializedPublicKey()))
                  .setSignature(ByteString.copyFrom(kemPreKeys.get(1L).signature()))
                  .build())
              .build())
          .putPreKeys(2, GetPreKeysResponse.PreKeyBundle.newBuilder()
              .setEcSignedPreKey(EcSignedPreKey.newBuilder()
                  .setKeyId(ecSignedPreKeys.get(2L).keyId())
                  .setPublicKey(ByteString.copyFrom(ecSignedPreKeys.get(2L).serializedPublicKey()))
                  .setSignature(ByteString.copyFrom(ecSignedPreKeys.get(2L).signature()))
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

    @SuppressWarnings("ResultOfMethodCallIgnored") final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class, () -> keysStub.getPreKeys(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(UUIDUtil.toByteString(UUID.randomUUID()))
                .build())
            .build()));

    assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());
  }

  @ParameterizedTest
  @ValueSource(longs = {KeysGrpcHelper.ALL_DEVICES, 1})
  void getPreKeysDeviceNotFound(final long deviceId) {
    final UUID accountIdentifier = UUID.randomUUID();

    final Account targetAccount = mock(Account.class);
    when(targetAccount.getUuid()).thenReturn(accountIdentifier);
    when(targetAccount.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(Curve.generateKeyPair().getPublicKey()));
    when(targetAccount.getDevices()).thenReturn(Collections.emptyList());
    when(targetAccount.getDevice(anyLong())).thenReturn(Optional.empty());

    when(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(accountIdentifier)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    @SuppressWarnings("ResultOfMethodCallIgnored") final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class, () -> keysStub.getPreKeys(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(UUIDUtil.toByteString(accountIdentifier))
                .build())
            .setDeviceId(deviceId)
            .build()));

    assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());
  }

  @Test
  void getPreKeysRateLimited() {
    final Account targetAccount = mock(Account.class);
    when(targetAccount.getUuid()).thenReturn(UUID.randomUUID());
    when(targetAccount.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(Curve.generateKeyPair().getPublicKey()));
    when(targetAccount.getDevices()).thenReturn(Collections.emptyList());
    when(targetAccount.getDevice(anyLong())).thenReturn(Optional.empty());

    when(accountsManager.getByServiceIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    final Duration retryAfterDuration = Duration.ofMinutes(7);

    when(preKeysRateLimiter.validateReactive(anyString()))
        .thenReturn(Mono.error(new RateLimitExceededException(retryAfterDuration, false)));

    @SuppressWarnings("ResultOfMethodCallIgnored") final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class, () -> keysStub.getPreKeys(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(UUIDUtil.toByteString(UUID.randomUUID()))
                .build())
            .build()));

    assertEquals(Status.Code.RESOURCE_EXHAUSTED, exception.getStatus().getCode());
    assertNotNull(exception.getTrailers());
    assertEquals(retryAfterDuration, exception.getTrailers().get(RateLimitUtil.RETRY_AFTER_DURATION_KEY));
  }
}
