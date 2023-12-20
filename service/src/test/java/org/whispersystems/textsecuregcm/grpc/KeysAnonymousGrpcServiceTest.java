/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusException;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.signal.chat.common.EcPreKey;
import org.signal.chat.common.EcSignedPreKey;
import org.signal.chat.common.KemSignedPreKey;
import org.signal.chat.common.ServiceIdentifier;
import org.signal.chat.keys.CheckIdentityKeyRequest;
import org.signal.chat.keys.GetPreKeysAnonymousRequest;
import org.signal.chat.keys.GetPreKeysRequest;
import org.signal.chat.keys.GetPreKeysResponse;
import org.signal.chat.keys.KeysAnonymousGrpc;
import org.signal.chat.keys.ReactorKeysAnonymousGrpc;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;

class KeysAnonymousGrpcServiceTest extends SimpleBaseGrpcTest<KeysAnonymousGrpcService, KeysAnonymousGrpc.KeysAnonymousBlockingStub> {

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private KeysManager keysManager;

  @Override
  protected KeysAnonymousGrpcService createServiceBeforeEachTest() {
    return new KeysAnonymousGrpcService(accountsManager, keysManager);
  }

  @Test
  void getPreKeys() {
    final Account targetAccount = mock(Account.class);
    final Device targetDevice = mock(Device.class);

    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());
    final UUID identifier = UUID.randomUUID();

    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    when(targetDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(targetDevice.isEnabled()).thenReturn(true);
    when(targetAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(targetDevice));

    when(targetAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(targetAccount.getIdentifier(IdentityType.ACI)).thenReturn(identifier);
    when(targetAccount.getIdentityKey(IdentityType.ACI)).thenReturn(identityKey);
    when(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(identifier)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    final ECPreKey ecPreKey = new ECPreKey(1, Curve.generateKeyPair().getPublicKey());
    final ECSignedPreKey ecSignedPreKey = KeysHelper.signedECPreKey(2, identityKeyPair);
    final KEMSignedPreKey kemSignedPreKey = KeysHelper.signedKEMPreKey(3, identityKeyPair);

    when(keysManager.takeEC(identifier, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(ecPreKey)));

    when(keysManager.takePQ(identifier, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(kemSignedPreKey)));

    when(keysManager.getEcSignedPreKey(identifier, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(ecSignedPreKey)));

    final GetPreKeysResponse response = unauthenticatedServiceStub().getPreKeys(GetPreKeysAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(UUIDUtil.toByteString(identifier))
                .build())
            .setDeviceId(Device.PRIMARY_ID)
            .build())
        .build());

    final GetPreKeysResponse expectedResponse = GetPreKeysResponse.newBuilder()
        .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
        .putPreKeys(Device.PRIMARY_ID, GetPreKeysResponse.PreKeyBundle.newBuilder()
            .setEcOneTimePreKey(EcPreKey.newBuilder()
                .setKeyId(ecPreKey.keyId())
                .setPublicKey(ByteString.copyFrom(ecPreKey.serializedPublicKey()))
                .build())
            .setEcSignedPreKey(EcSignedPreKey.newBuilder()
                .setKeyId(ecSignedPreKey.keyId())
                .setPublicKey(ByteString.copyFrom(ecSignedPreKey.serializedPublicKey()))
                .setSignature(ByteString.copyFrom(ecSignedPreKey.signature()))
                .build())
            .setKemOneTimePreKey(KemSignedPreKey.newBuilder()
                .setKeyId(kemSignedPreKey.keyId())
                .setPublicKey(ByteString.copyFrom(kemSignedPreKey.serializedPublicKey()))
                .setSignature(ByteString.copyFrom(kemSignedPreKey.signature()))
                .build())
            .build())
        .build();

    assertEquals(expectedResponse, response);
  }

  @Test
  void getPreKeysIncorrectUnidentifiedAccessKey() {
    final Account targetAccount = mock(Account.class);

    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());
    final UUID identifier = UUID.randomUUID();

    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    when(targetAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(targetAccount.getUuid()).thenReturn(identifier);
    when(targetAccount.getIdentityKey(IdentityType.ACI)).thenReturn(identityKey);
    when(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(identifier)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    assertStatusException(Status.UNAUTHENTICATED, () -> unauthenticatedServiceStub().getPreKeys(GetPreKeysAnonymousRequest.newBuilder()
        .setRequest(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(UUIDUtil.toByteString(identifier))
                .build())
            .setDeviceId(Device.PRIMARY_ID)
            .build())
        .build()));
  }

  @Test
  void getPreKeysAccountNotFound() {
    when(accountsManager.getByServiceIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class,
            () -> unauthenticatedServiceStub().getPreKeys(GetPreKeysAnonymousRequest.newBuilder()
                .setUnidentifiedAccessKey(UUIDUtil.toByteString(UUID.randomUUID()))
                .setRequest(GetPreKeysRequest.newBuilder()
                    .setTargetIdentifier(ServiceIdentifier.newBuilder()
                        .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
                        .setUuid(UUIDUtil.toByteString(UUID.randomUUID()))
                        .build())
                    .build())
                .build()));

    assertEquals(Status.Code.UNAUTHENTICATED, exception.getStatus().getCode());
  }

  @ParameterizedTest
  @ValueSource(bytes = {KeysGrpcHelper.ALL_DEVICES, 1})
  void getPreKeysDeviceNotFound(final byte deviceId) {
    final UUID accountIdentifier = UUID.randomUUID();

    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    final Account targetAccount = mock(Account.class);
    when(targetAccount.getUuid()).thenReturn(accountIdentifier);
    when(targetAccount.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(Curve.generateKeyPair().getPublicKey()));
    when(targetAccount.getDevices()).thenReturn(Collections.emptyList());
    when(targetAccount.getDevice(anyByte())).thenReturn(Optional.empty());
    when(targetAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));

    when(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(accountIdentifier)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    assertStatusException(Status.NOT_FOUND, () -> unauthenticatedServiceStub().getPreKeys(GetPreKeysAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(UUIDUtil.toByteString(accountIdentifier))
                .build())
            .setDeviceId(deviceId)
            .build())
        .build()));
  }

  @Test
  void checkIdentityKeys() {
    final ReactorKeysAnonymousGrpc.ReactorKeysAnonymousStub reactiveKeysAnonymousStub = ReactorKeysAnonymousGrpc.newReactorStub(SimpleBaseGrpcTest.GRPC_SERVER_EXTENSION_UNAUTHENTICATED.getChannel());
    when(accountsManager.getByServiceIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    final Account mismatchedAciFingerprintAccount = mock(Account.class);
    final UUID mismatchedAciFingerprintAccountIdentifier = UUID.randomUUID();
    final IdentityKey mismatchedAciFingerprintAccountIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    final Account matchingAciFingerprintAccount = mock(Account.class);
    final UUID matchingAciFingerprintAccountIdentifier = UUID.randomUUID();
    final IdentityKey matchingAciFingerprintAccountIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    final Account mismatchedPniFingerprintAccount = mock(Account.class);
    final UUID mismatchedPniFingerprintAccountIdentifier = UUID.randomUUID();
    final IdentityKey mismatchedPniFingerpringAccountIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    when(mismatchedAciFingerprintAccount.getIdentityKey(IdentityType.ACI)).thenReturn(mismatchedAciFingerprintAccountIdentityKey);
    when(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(mismatchedAciFingerprintAccountIdentifier)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(mismatchedAciFingerprintAccount)));

    when(matchingAciFingerprintAccount.getIdentityKey(IdentityType.ACI)).thenReturn(matchingAciFingerprintAccountIdentityKey);
    when(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(matchingAciFingerprintAccountIdentifier)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(matchingAciFingerprintAccount)));

    when(mismatchedPniFingerprintAccount.getIdentityKey(IdentityType.PNI)).thenReturn(mismatchedPniFingerpringAccountIdentityKey);
    when(accountsManager.getByServiceIdentifierAsync(new PniServiceIdentifier(mismatchedPniFingerprintAccountIdentifier)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(mismatchedPniFingerprintAccount)));

    final Flux<CheckIdentityKeyRequest> requests = Flux.just(
        buildCheckIdentityKeyRequest(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI, mismatchedAciFingerprintAccountIdentifier,
            new IdentityKey(Curve.generateKeyPair().getPublicKey())),
        buildCheckIdentityKeyRequest(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI, matchingAciFingerprintAccountIdentifier,
            matchingAciFingerprintAccountIdentityKey),
        buildCheckIdentityKeyRequest(org.signal.chat.common.IdentityType.IDENTITY_TYPE_PNI, UUID.randomUUID(),
            new IdentityKey(Curve.generateKeyPair().getPublicKey())),
        buildCheckIdentityKeyRequest(org.signal.chat.common.IdentityType.IDENTITY_TYPE_PNI, mismatchedPniFingerprintAccountIdentifier,
            new IdentityKey(Curve.generateKeyPair().getPublicKey()))
    );

    final Map<UUID, IdentityKey> expectedResponses = Map.of(
        mismatchedAciFingerprintAccountIdentifier, mismatchedAciFingerprintAccountIdentityKey,
        mismatchedPniFingerprintAccountIdentifier, mismatchedPniFingerpringAccountIdentityKey);

    final Map<UUID, IdentityKey> responses = reactiveKeysAnonymousStub.checkIdentityKeys(requests)
        .collectMap(response -> ServiceIdentifierUtil.fromGrpcServiceIdentifier(response.getTargetIdentifier()).uuid(),
            response -> {
              try {
                return new IdentityKey(response.getIdentityKey().toByteArray());
              } catch (InvalidKeyException e) {
                throw new RuntimeException(e);
              }
            })
        .block();

    assertEquals(expectedResponses, responses);
  }

  private static CheckIdentityKeyRequest buildCheckIdentityKeyRequest(final org.signal.chat.common.IdentityType identityType,
      final UUID uuid, final IdentityKey identityKey) {
    return CheckIdentityKeyRequest.newBuilder()
        .setTargetIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(identityType)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(uuid))))
        .setFingerprint(ByteString.copyFrom(getFingerprint(identityKey)))
        .build();
  }

  private static byte[] getFingerprint(final IdentityKey publicKey) {
    try {
      return Util.truncate(MessageDigest.getInstance("SHA-256").digest(publicKey.serialize()), 4);
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError("All Java implementations must support SHA-256 MessageDigest algorithm", e);
    }
  }
}
