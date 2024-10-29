/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusException;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
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
import org.signal.libsignal.zkgroup.ServerSecretParams;
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
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;

class KeysAnonymousGrpcServiceTest extends SimpleBaseGrpcTest<KeysAnonymousGrpcService, KeysAnonymousGrpc.KeysAnonymousBlockingStub> {

  private static final ServerSecretParams SERVER_SECRET_PARAMS = ServerSecretParams.generate();
  private static final TestClock CLOCK = TestClock.now();

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private KeysManager keysManager;

  @Override
  protected KeysAnonymousGrpcService createServiceBeforeEachTest() {
    return new KeysAnonymousGrpcService(accountsManager, keysManager, SERVER_SECRET_PARAMS, CLOCK);
  }

  @Test
  void getPreKeysUnidentifiedAccessKey() {
    final Account targetAccount = mock(Account.class);

    final Device targetDevice = DevicesHelper.createDevice(Device.PRIMARY_ID);
    when(targetAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(targetDevice));

    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());
    final UUID uuid = UUID.randomUUID();
    final AciServiceIdentifier identifier = new AciServiceIdentifier(uuid);
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    when(targetAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(targetAccount.getIdentifier(IdentityType.ACI)).thenReturn(uuid);
    when(targetAccount.getIdentityKey(IdentityType.ACI)).thenReturn(identityKey);
    when(accountsManager.getByServiceIdentifierAsync(identifier))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    final ECPreKey ecPreKey = new ECPreKey(1, Curve.generateKeyPair().getPublicKey());
    final ECSignedPreKey ecSignedPreKey = KeysHelper.signedECPreKey(2, identityKeyPair);
    final KEMSignedPreKey kemSignedPreKey = KeysHelper.signedKEMPreKey(3, identityKeyPair);

    when(keysManager.takeEC(uuid, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(ecPreKey)));

    when(keysManager.takePQ(uuid, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(kemSignedPreKey)));

    when(keysManager.getEcSignedPreKey(uuid, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(ecSignedPreKey)));

    final GetPreKeysResponse response = unauthenticatedServiceStub().getPreKeys(GetPreKeysAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(identifier))
            .setDeviceId(Device.PRIMARY_ID))
        .build());

    final GetPreKeysResponse expectedResponse = GetPreKeysResponse.newBuilder()
        .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
        .putPreKeys(Device.PRIMARY_ID, GetPreKeysResponse.PreKeyBundle.newBuilder()
            .setEcOneTimePreKey(toGrpcEcPreKey(ecPreKey))
            .setEcSignedPreKey(toGrpcEcSignedPreKey(ecSignedPreKey))
            .setKemOneTimePreKey(toGrpcKemSignedPreKey(kemSignedPreKey))
            .build())
        .build();

    assertEquals(expectedResponse, response);
  }

  @Test
  void getPreKeysGroupSendEndorsement() throws Exception {
    final Account targetAccount = mock(Account.class);

    final Device targetDevice = DevicesHelper.createDevice(Device.PRIMARY_ID);
    when(targetAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(targetDevice));

    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());
    final UUID uuid = UUID.randomUUID();
    final AciServiceIdentifier identifier = new AciServiceIdentifier(uuid);
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    when(targetAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(targetAccount.getIdentifier(IdentityType.ACI)).thenReturn(uuid);
    when(targetAccount.getIdentityKey(IdentityType.ACI)).thenReturn(identityKey);
    when(accountsManager.getByServiceIdentifierAsync(identifier))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    final ECPreKey ecPreKey = new ECPreKey(1, Curve.generateKeyPair().getPublicKey());
    final ECSignedPreKey ecSignedPreKey = KeysHelper.signedECPreKey(2, identityKeyPair);
    final KEMSignedPreKey kemSignedPreKey = KeysHelper.signedKEMPreKey(3, identityKeyPair);

    when(keysManager.takeEC(uuid, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(ecPreKey)));

    when(keysManager.takePQ(uuid, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(kemSignedPreKey)));

    when(keysManager.getEcSignedPreKey(uuid, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(ecSignedPreKey)));

    // Expirations must be on day boundaries or libsignal will refuse to create or verify the token
    final Instant expiration = Instant.now().truncatedTo(ChronoUnit.DAYS);
    CLOCK.pin(expiration.minus(Duration.ofHours(1))); // set time so the credential isn't expired yet
    final byte[] token = AuthHelper.validGroupSendToken(SERVER_SECRET_PARAMS, List.of(identifier), expiration);

    final GetPreKeysResponse response = unauthenticatedServiceStub().getPreKeys(GetPreKeysAnonymousRequest.newBuilder()
        .setGroupSendToken(ByteString.copyFrom(token))
        .setRequest(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(identifier))
            .setDeviceId(Device.PRIMARY_ID))
        .build());

    final GetPreKeysResponse expectedResponse = GetPreKeysResponse.newBuilder()
        .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
        .putPreKeys(Device.PRIMARY_ID, GetPreKeysResponse.PreKeyBundle.newBuilder()
            .setEcOneTimePreKey(toGrpcEcPreKey(ecPreKey))
            .setEcSignedPreKey(toGrpcEcSignedPreKey(ecSignedPreKey))
            .setKemOneTimePreKey(toGrpcKemSignedPreKey(kemSignedPreKey))
            .build())
        .build();

    assertEquals(expectedResponse, response);
  }

  @Test
  void getPreKeysNoAuth() {
    assertGetKeysFailure(Status.INVALID_ARGUMENT, GetPreKeysAnonymousRequest.newBuilder()
        .setRequest(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(UUID.randomUUID())))
            .setDeviceId(Device.PRIMARY_ID))
        .build());

    verifyNoInteractions(accountsManager);
    verifyNoInteractions(keysManager);
  }

  @Test
  void getPreKeysIncorrectUnidentifiedAccessKey() {
    final Account targetAccount = mock(Account.class);

    final UUID uuid = UUID.randomUUID();
    final AciServiceIdentifier identifier = new AciServiceIdentifier(uuid);
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    when(targetAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(accountsManager.getByServiceIdentifierAsync(identifier))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    assertGetKeysFailure(Status.UNAUTHENTICATED, GetPreKeysAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(UUIDUtil.toByteString(UUID.randomUUID()))
        .setRequest(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(identifier))
            .setDeviceId(Device.PRIMARY_ID))
        .build());

    verifyNoInteractions(keysManager);
  }

  @Test
  void getPreKeysExpiredGroupSendEndorsement() throws Exception {
    final UUID uuid = UUID.randomUUID();
    final AciServiceIdentifier identifier = new AciServiceIdentifier(uuid);

    // Expirations must be on day boundaries or libsignal will refuse to create or verify the token
    final Instant expiration = Instant.now().truncatedTo(ChronoUnit.DAYS);
    CLOCK.pin(expiration.plus(Duration.ofHours(1))); // set time so our token is already expired

    final byte[] token = AuthHelper.validGroupSendToken(SERVER_SECRET_PARAMS, List.of(identifier), expiration);

    assertGetKeysFailure(Status.UNAUTHENTICATED, GetPreKeysAnonymousRequest.newBuilder()
        .setGroupSendToken(ByteString.copyFrom(token))
        .setRequest(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(identifier))
            .setDeviceId(Device.PRIMARY_ID))
        .build());

    verifyNoInteractions(accountsManager);
    verifyNoInteractions(keysManager);
  }

  @Test
  void getPreKeysIncorrectGroupSendEndorsement() throws Exception {
    final AciServiceIdentifier authorizedIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final AciServiceIdentifier targetIdentifier = new AciServiceIdentifier(UUID.randomUUID());

    // Expirations must be on day boundaries or libsignal will refuse to create or verify the token
    final Instant expiration = Instant.now().truncatedTo(ChronoUnit.DAYS);
    CLOCK.pin(expiration.minus(Duration.ofHours(1))); // set time so the credential isn't expired yet

    final AciServiceIdentifier wrongAci = new AciServiceIdentifier(UUID.randomUUID());
    final byte[] token = AuthHelper.validGroupSendToken(SERVER_SECRET_PARAMS, List.of(authorizedIdentifier), expiration);

    assertGetKeysFailure(Status.UNAUTHENTICATED, GetPreKeysAnonymousRequest.newBuilder()
        .setGroupSendToken(ByteString.copyFrom(token))
        .setRequest(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(targetIdentifier))
            .setDeviceId(Device.PRIMARY_ID))
        .build());

    verifyNoInteractions(accountsManager);
    verifyNoInteractions(keysManager);
  }

  @Test
  void getPreKeysAccountNotFoundUnidentifiedAccessKey() {
    final AciServiceIdentifier nonexistentAci = new AciServiceIdentifier(UUID.randomUUID());
    when(accountsManager.getByServiceIdentifierAsync(nonexistentAci))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    assertGetKeysFailure(Status.UNAUTHENTICATED,
        GetPreKeysAnonymousRequest.newBuilder()
            .setUnidentifiedAccessKey(UUIDUtil.toByteString(UUID.randomUUID()))
            .setRequest(GetPreKeysRequest.newBuilder()
                .setTargetIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(nonexistentAci)))
        .build());

    verifyNoInteractions(keysManager);
  }

  @Test
  void getPreKeysAccountNotFoundGroupSendEndorsement() throws Exception {
    final AciServiceIdentifier nonexistentAci = new AciServiceIdentifier(UUID.randomUUID());

    // Expirations must be on day boundaries or libsignal will refuse to create or verify the token
    final Instant expiration = Instant.now().truncatedTo(ChronoUnit.DAYS);
    CLOCK.pin(expiration.minus(Duration.ofHours(1))); // set time so the credential isn't expired yet

    final byte[] token = AuthHelper.validGroupSendToken(SERVER_SECRET_PARAMS, List.of(nonexistentAci), expiration);

    when(accountsManager.getByServiceIdentifierAsync(nonexistentAci))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    assertGetKeysFailure(Status.NOT_FOUND,
        GetPreKeysAnonymousRequest.newBuilder()
            .setGroupSendToken(ByteString.copyFrom(token))
            .setRequest(GetPreKeysRequest.newBuilder()
                .setTargetIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(nonexistentAci)))
        .build());

    verifyNoInteractions(keysManager);
  }

  @Test
  void getPreKeysDeviceNotFound() {
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

    assertGetKeysFailure(Status.NOT_FOUND, GetPreKeysAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(UUIDUtil.toByteString(accountIdentifier)))
            .setDeviceId(Device.PRIMARY_ID))
        .build());
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

  private void assertGetKeysFailure(Status code, GetPreKeysAnonymousRequest request) {
    assertStatusException(code, () -> unauthenticatedServiceStub().getPreKeys(request));
  }

  private static EcPreKey toGrpcEcPreKey(final ECPreKey preKey) {
    return EcPreKey.newBuilder()
        .setKeyId(preKey.keyId())
        .setPublicKey(ByteString.copyFrom(preKey.publicKey().serialize()))
        .build();
  }

  private static EcSignedPreKey toGrpcEcSignedPreKey(final ECSignedPreKey preKey) {
    return EcSignedPreKey.newBuilder()
        .setKeyId(preKey.keyId())
        .setPublicKey(ByteString.copyFrom(preKey.publicKey().serialize()))
        .setSignature(ByteString.copyFrom(preKey.signature()))
        .build();
  }

  private static KemSignedPreKey toGrpcKemSignedPreKey(final KEMSignedPreKey preKey) {
    return KemSignedPreKey.newBuilder()
        .setKeyId(preKey.keyId())
        .setPublicKey(ByteString.copyFrom(preKey.publicKey().serialize()))
        .setSignature(ByteString.copyFrom(preKey.signature()))
        .build();
  }

}
