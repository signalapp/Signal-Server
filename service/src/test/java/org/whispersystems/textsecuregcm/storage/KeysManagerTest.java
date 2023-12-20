/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;

class KeysManagerTest {

  private KeysManager keysManager;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      Tables.EC_KEYS, Tables.PQ_KEYS, Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS, Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS);

  private static final UUID ACCOUNT_UUID = UUID.randomUUID();
  private static final byte DEVICE_ID = 1;

  private static final ECKeyPair IDENTITY_KEY_PAIR = Curve.generateKeyPair();

  @BeforeEach
  void setup() {
    keysManager = new KeysManager(
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        Tables.EC_KEYS.tableName(),
        Tables.PQ_KEYS.tableName(),
        Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS.tableName(),
        Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS.tableName()
    );
  }

  @Test
  void storeEcOneTimePreKeys() {
    assertEquals(0, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Initial pre-key count for an account should be zero");

    keysManager.storeEcOneTimePreKeys(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestPreKey(1))).join();
    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join());

    keysManager.storeEcOneTimePreKeys(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestPreKey(1))).join();
    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Repeatedly storing same key should have no effect");
  }

  @Test
  void storeKemOneTimePreKeys() {
    assertEquals(0, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Initial pre-key count for an account should be zero");

    keysManager.storeKemOneTimePreKeys(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestKEMSignedPreKey(1))).join();
    assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());

    keysManager.storeKemOneTimePreKeys(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestKEMSignedPreKey(1))).join();
    assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());
  }

  @Test
  void storeEcSignedPreKeys() {
    assertTrue(keysManager.getEcSignedPreKey(ACCOUNT_UUID, DEVICE_ID).join().isEmpty());

    final ECSignedPreKey signedPreKey = generateTestECSignedPreKey(1);

    keysManager.storeEcSignedPreKeys(ACCOUNT_UUID, DEVICE_ID, signedPreKey).join();

    assertEquals(Optional.of(signedPreKey), keysManager.getEcSignedPreKey(ACCOUNT_UUID, DEVICE_ID).join());
  }

  @Test
  void testTakeAccountAndDeviceId() {
    assertEquals(Optional.empty(), keysManager.takeEC(ACCOUNT_UUID, DEVICE_ID).join());

    final ECPreKey preKey = generateTestPreKey(1);

    keysManager.storeEcOneTimePreKeys(ACCOUNT_UUID, DEVICE_ID, List.of(preKey, generateTestPreKey(2))).join();

    final Optional<ECPreKey> takenKey = keysManager.takeEC(ACCOUNT_UUID, DEVICE_ID).join();
    assertEquals(Optional.of(preKey), takenKey);
    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join());
  }

  @Test
  void testTakePQ() {
    assertEquals(Optional.empty(), keysManager.takeEC(ACCOUNT_UUID, DEVICE_ID).join());

    final KEMSignedPreKey preKey1 = generateTestKEMSignedPreKey(1);
    final KEMSignedPreKey preKey2 = generateTestKEMSignedPreKey(2);
    final KEMSignedPreKey preKeyLast = generateTestKEMSignedPreKey(1001);

    keysManager.storeKemOneTimePreKeys(ACCOUNT_UUID, DEVICE_ID, List.of(preKey1, preKey2)).join();
    keysManager.storePqLastResort(ACCOUNT_UUID, DEVICE_ID, preKeyLast).join();

    assertEquals(Optional.of(preKey1), keysManager.takePQ(ACCOUNT_UUID, DEVICE_ID).join());
    assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());

    assertEquals(Optional.of(preKey2), keysManager.takePQ(ACCOUNT_UUID, DEVICE_ID).join());
    assertEquals(0, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());

    assertEquals(Optional.of(preKeyLast), keysManager.takePQ(ACCOUNT_UUID, DEVICE_ID).join());
    assertEquals(0, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());

    assertEquals(Optional.of(preKeyLast), keysManager.takePQ(ACCOUNT_UUID, DEVICE_ID).join());
    assertEquals(0, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());
  }

  @Test
  void testDeleteSingleUsePreKeysByAccount() {
    int keyId = 1;

    for (byte deviceId : new byte[] {DEVICE_ID, DEVICE_ID + 1}) {
      keysManager.storeEcOneTimePreKeys(ACCOUNT_UUID, deviceId, List.of(generateTestPreKey(keyId++))).join();
      keysManager.storeKemOneTimePreKeys(ACCOUNT_UUID, deviceId, List.of(generateTestKEMSignedPreKey(keyId++))).join();
      keysManager.storeEcSignedPreKeys(ACCOUNT_UUID, deviceId, generateTestECSignedPreKey(keyId++)).join();
      keysManager.storePqLastResort(ACCOUNT_UUID, deviceId, generateTestKEMSignedPreKey(keyId++)).join();
    }

    for (byte deviceId : new byte[] {DEVICE_ID, DEVICE_ID + 1}) {
      assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, deviceId).join());
      assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, deviceId).join());
      assertTrue(keysManager.getEcSignedPreKey(ACCOUNT_UUID, deviceId).join().isPresent());
      assertTrue(keysManager.getLastResort(ACCOUNT_UUID, deviceId).join().isPresent());
    }

    keysManager.deleteSingleUsePreKeys(ACCOUNT_UUID).join();

    for (byte deviceId : new byte[] {DEVICE_ID, DEVICE_ID + 1}) {
      assertEquals(0, keysManager.getEcCount(ACCOUNT_UUID, deviceId).join());
      assertEquals(0, keysManager.getPqCount(ACCOUNT_UUID, deviceId).join());
      assertTrue(keysManager.getEcSignedPreKey(ACCOUNT_UUID, deviceId).join().isPresent());
      assertTrue(keysManager.getLastResort(ACCOUNT_UUID, deviceId).join().isPresent());
    }
  }

  @Test
  void testDeleteSingleUsePreKeysByAccountAndDevice() {
    int keyId = 1;

    for (byte deviceId : new byte[] {DEVICE_ID, DEVICE_ID + 1}) {
      keysManager.storeEcOneTimePreKeys(ACCOUNT_UUID, deviceId, List.of(generateTestPreKey(keyId++))).join();
      keysManager.storeKemOneTimePreKeys(ACCOUNT_UUID, deviceId, List.of(generateTestKEMSignedPreKey(keyId++))).join();
      keysManager.storeEcSignedPreKeys(ACCOUNT_UUID, deviceId, generateTestECSignedPreKey(keyId++)).join();
      keysManager.storePqLastResort(ACCOUNT_UUID, deviceId, generateTestKEMSignedPreKey(keyId++)).join();
    }

    for (byte deviceId : new byte[] {DEVICE_ID, DEVICE_ID + 1}) {
      assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, deviceId).join());
      assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, deviceId).join());
      assertTrue(keysManager.getEcSignedPreKey(ACCOUNT_UUID, deviceId).join().isPresent());
      assertTrue(keysManager.getLastResort(ACCOUNT_UUID, deviceId).join().isPresent());
    }

    keysManager.deleteSingleUsePreKeys(ACCOUNT_UUID, DEVICE_ID).join();

    assertEquals(0, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertEquals(0, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertTrue(keysManager.getEcSignedPreKey(ACCOUNT_UUID, DEVICE_ID).join().isPresent());
    assertTrue(keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID).join().isPresent());

    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, (byte) (DEVICE_ID + 1)).join());
    assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, (byte) (DEVICE_ID + 1)).join());
    assertTrue(keysManager.getEcSignedPreKey(ACCOUNT_UUID, (byte) (DEVICE_ID + 1)).join().isPresent());
    assertTrue(keysManager.getLastResort(ACCOUNT_UUID, (byte) (DEVICE_ID + 1)).join().isPresent());
  }

  @Test
  void testStorePqLastResort() {
    assertEquals(0, keysManager.getPqEnabledDevices(ACCOUNT_UUID).join().size());

    final ECKeyPair identityKeyPair = Curve.generateKeyPair();

    final byte deviceId2 = 2;
    final byte deviceId3 = 3;

    keysManager.storePqLastResort(ACCOUNT_UUID, DEVICE_ID, KeysHelper.signedKEMPreKey(1, identityKeyPair)).join();
    keysManager.storePqLastResort(ACCOUNT_UUID, (byte) 2, KeysHelper.signedKEMPreKey(2, identityKeyPair)).join();

    assertEquals(2, keysManager.getPqEnabledDevices(ACCOUNT_UUID).join().size());
    assertEquals(1L, keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID).join().get().keyId());
    assertEquals(2L, keysManager.getLastResort(ACCOUNT_UUID, deviceId2).join().get().keyId());
    assertFalse(keysManager.getLastResort(ACCOUNT_UUID, deviceId3).join().isPresent());

    keysManager.storePqLastResort(ACCOUNT_UUID, DEVICE_ID, KeysHelper.signedKEMPreKey(3, identityKeyPair)).join();
    keysManager.storePqLastResort(ACCOUNT_UUID, deviceId3, KeysHelper.signedKEMPreKey(4, identityKeyPair)).join();

    assertEquals(3, keysManager.getPqEnabledDevices(ACCOUNT_UUID).join().size(), "storing new last-resort keys should not create duplicates");
    assertEquals(3L, keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID).join().get().keyId(),
        "storing new last-resort keys should overwrite old ones");
    assertEquals(2L, keysManager.getLastResort(ACCOUNT_UUID, deviceId2).join().get().keyId(),
        "storing new last-resort keys should leave untouched ones alone");
    assertEquals(4L, keysManager.getLastResort(ACCOUNT_UUID, deviceId3).join().get().keyId(),
        "storing new last-resort keys should overwrite old ones");
  }

  @Test
  void testGetPqEnabledDevices() {
    keysManager.storeKemOneTimePreKeys(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestKEMSignedPreKey(1))).join();
    keysManager.storePqLastResort(ACCOUNT_UUID, (byte) (DEVICE_ID + 1), generateTestKEMSignedPreKey(2)).join();
    keysManager.storeKemOneTimePreKeys(ACCOUNT_UUID, (byte) (DEVICE_ID + 2), List.of(generateTestKEMSignedPreKey(3))).join();
    keysManager.storePqLastResort(ACCOUNT_UUID, (byte) (DEVICE_ID + 2), generateTestKEMSignedPreKey(4)).join();

    assertIterableEquals(
        Set.of((byte) (DEVICE_ID + 1), (byte) (DEVICE_ID + 2)),
        Set.copyOf(keysManager.getPqEnabledDevices(ACCOUNT_UUID).join()));
  }

  private static ECPreKey generateTestPreKey(final long keyId) {
    return new ECPreKey(keyId, Curve.generateKeyPair().getPublicKey());
  }

  private static ECSignedPreKey generateTestECSignedPreKey(final long keyId) {
    return KeysHelper.signedECPreKey(keyId, IDENTITY_KEY_PAIR);
  }

  private static KEMSignedPreKey generateTestKEMSignedPreKey(final long keyId) {
    return KeysHelper.signedKEMPreKey(keyId, IDENTITY_KEY_PAIR);
  }
}
