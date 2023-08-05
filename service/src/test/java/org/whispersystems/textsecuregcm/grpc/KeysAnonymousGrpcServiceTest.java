/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.chat.common.EcPreKey;
import org.signal.chat.common.EcSignedPreKey;
import org.signal.chat.common.KemSignedPreKey;
import org.signal.chat.common.ServiceIdentifier;
import org.signal.chat.keys.GetPreKeysAnonymousRequest;
import org.signal.chat.keys.GetPreKeysRequest;
import org.signal.chat.keys.GetPreKeysResponse;
import org.signal.chat.keys.KeysAnonymousGrpc;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class KeysAnonymousGrpcServiceTest {

  private AccountsManager accountsManager;
  private KeysManager keysManager;

  private KeysAnonymousGrpc.KeysAnonymousBlockingStub keysAnonymousStub;

  @RegisterExtension
  static final GrpcServerExtension GRPC_SERVER_EXTENSION = new GrpcServerExtension();

  @BeforeEach
  void setUp() {
    accountsManager = mock(AccountsManager.class);
    keysManager = mock(KeysManager.class);

    final KeysAnonymousGrpcService keysGrpcService =
        new KeysAnonymousGrpcService(accountsManager, keysManager);

    keysAnonymousStub = KeysAnonymousGrpc.newBlockingStub(GRPC_SERVER_EXTENSION.getChannel());

    GRPC_SERVER_EXTENSION.getServiceRegistry().addService(keysGrpcService);
  }

  @Test
  void getPreKeys() {
    final Account targetAccount = mock(Account.class);
    final Device targetDevice = mock(Device.class);

    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());
    final UUID identifier = UUID.randomUUID();

    final byte[] unidentifiedAccessKey = new byte[16];
    new SecureRandom().nextBytes(unidentifiedAccessKey);

    when(targetDevice.getId()).thenReturn(Device.MASTER_ID);
    when(targetDevice.isEnabled()).thenReturn(true);
    when(targetAccount.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(targetDevice));

    when(targetAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(targetAccount.getIdentifier(IdentityType.ACI)).thenReturn(identifier);
    when(targetAccount.getIdentityKey(IdentityType.ACI)).thenReturn(identityKey);
    when(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(identifier)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    final ECPreKey ecPreKey = new ECPreKey(1, Curve.generateKeyPair().getPublicKey());
    final ECSignedPreKey ecSignedPreKey = KeysHelper.signedECPreKey(2, identityKeyPair);
    final KEMSignedPreKey kemSignedPreKey = KeysHelper.signedKEMPreKey(3, identityKeyPair);

    when(keysManager.takeEC(identifier, Device.MASTER_ID)).thenReturn(CompletableFuture.completedFuture(Optional.of(ecPreKey)));
    when(keysManager.takePQ(identifier, Device.MASTER_ID)).thenReturn(CompletableFuture.completedFuture(Optional.of(kemSignedPreKey)));
    when(targetDevice.getSignedPreKey(IdentityType.ACI)).thenReturn(ecSignedPreKey);

    final GetPreKeysResponse response = keysAnonymousStub.getPreKeys(GetPreKeysAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetPreKeysRequest.newBuilder()
            .setTargetIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(UUIDUtil.toByteString(identifier))
                .build())
            .setDeviceId(Device.MASTER_ID)
            .build())
        .build());

    final GetPreKeysResponse expectedResponse = GetPreKeysResponse.newBuilder()
        .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
        .putPreKeys(Device.MASTER_ID, GetPreKeysResponse.PreKeyBundle.newBuilder()
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

    final byte[] unidentifiedAccessKey = new byte[16];
    new SecureRandom().nextBytes(unidentifiedAccessKey);

    when(targetAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(targetAccount.getUuid()).thenReturn(identifier);
    when(targetAccount.getIdentityKey(IdentityType.ACI)).thenReturn(identityKey);
    when(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(identifier)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    final StatusRuntimeException statusRuntimeException =
        assertThrows(StatusRuntimeException.class,
            () -> keysAnonymousStub.getPreKeys(GetPreKeysAnonymousRequest.newBuilder()
                .setRequest(GetPreKeysRequest.newBuilder()
                    .setTargetIdentifier(ServiceIdentifier.newBuilder()
                        .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
                        .setUuid(UUIDUtil.toByteString(identifier))
                        .build())
                    .setDeviceId(Device.MASTER_ID)
                    .build())
                .build()));

    assertEquals(Status.UNAUTHENTICATED.getCode(), statusRuntimeException.getStatus().getCode());
  }

  @Test
  void getPreKeysAccountNotFound() {
    when(accountsManager.getByServiceIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class,
            () -> keysAnonymousStub.getPreKeys(GetPreKeysAnonymousRequest.newBuilder()
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
  @ValueSource(longs = {KeysGrpcHelper.ALL_DEVICES, 1})
  void getPreKeysDeviceNotFound(final long deviceId) {
    final UUID accountIdentifier = UUID.randomUUID();

    final byte[] unidentifiedAccessKey = new byte[16];
    new SecureRandom().nextBytes(unidentifiedAccessKey);

    final Account targetAccount = mock(Account.class);
    when(targetAccount.getUuid()).thenReturn(accountIdentifier);
    when(targetAccount.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(Curve.generateKeyPair().getPublicKey()));
    when(targetAccount.getDevices()).thenReturn(Collections.emptyList());
    when(targetAccount.getDevice(anyLong())).thenReturn(Optional.empty());
    when(targetAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));

    when(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(accountIdentifier)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(targetAccount)));

    final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class,
            () -> keysAnonymousStub.getPreKeys(GetPreKeysAnonymousRequest.newBuilder()
                .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
                .setRequest(GetPreKeysRequest.newBuilder()
                    .setTargetIdentifier(ServiceIdentifier.newBuilder()
                        .setIdentityType(org.signal.chat.common.IdentityType.IDENTITY_TYPE_ACI)
                        .setUuid(UUIDUtil.toByteString(accountIdentifier))
                        .build())
                    .setDeviceId(deviceId)
                    .build())
                .build()));

    assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());
  }
}
