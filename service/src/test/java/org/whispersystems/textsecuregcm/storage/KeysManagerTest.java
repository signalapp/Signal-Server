/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicECPreKeyMigrationConfiguration;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;

class KeysManagerTest {

  private DynamicECPreKeyMigrationConfiguration ecPreKeyMigrationConfiguration;
  private KeysManager keysManager;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      Tables.EC_KEYS, Tables.PQ_KEYS, Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS, Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS);

  private static final UUID ACCOUNT_UUID = UUID.randomUUID();
  private static final long DEVICE_ID = 1L;

  private static final ECKeyPair IDENTITY_KEY_PAIR = Curve.generateKeyPair();

  @BeforeEach
  void setup() {
    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    ecPreKeyMigrationConfiguration = mock(DynamicECPreKeyMigrationConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getEcPreKeyMigrationConfiguration()).thenReturn(ecPreKeyMigrationConfiguration);
    when(ecPreKeyMigrationConfiguration.storeEcSignedPreKeys()).thenReturn(true);
    when(ecPreKeyMigrationConfiguration.deleteEcSignedPreKeys()).thenReturn(true);

    keysManager = new KeysManager(
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        Tables.EC_KEYS.tableName(),
        Tables.PQ_KEYS.tableName(),
        Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS.tableName(),
        Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS.tableName(),
        dynamicConfigurationManager);
  }

  @Test
  void testStore() {
    assertEquals(0, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Initial pre-key count for an account should be zero");
    assertEquals(0, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Initial pre-key count for an account should be zero");
    assertFalse(keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID).join().isPresent(),
        "Initial last-resort pre-key for an account should be missing");

    keysManager.store(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestPreKey(1)), null, null, null).join();
    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join());

    keysManager.store(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestPreKey(1)), null, null, null).join();
    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Repeatedly storing same key should have no effect");

    keysManager.store(ACCOUNT_UUID, DEVICE_ID, null, List.of(generateTestKEMSignedPreKey(1)), null, null).join();
    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Uploading new PQ prekeys should have no effect on EC prekeys");
    assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());

    keysManager.store(ACCOUNT_UUID, DEVICE_ID, null, null, null, generateTestKEMSignedPreKey(1001)).join();
    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Uploading new PQ last-resort prekey should have no effect on EC prekeys");
    assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Uploading new PQ last-resort prekey should have no effect on one-time PQ prekeys");
    assertEquals(1001, keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID).join().get().keyId());

    keysManager.store(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestPreKey(2)), null, null, null).join();
    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Inserting a new key should overwrite all prior keys of the same type for the given account/device");
    assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Uploading new EC prekeys should have no effect on PQ prekeys");

    keysManager.store(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestPreKey(3)), List.of(generateTestKEMSignedPreKey(2)), null, null).join();
    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Inserting a new key should overwrite all prior keys of the same type for the given account/device");
    assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Inserting a new key should overwrite all prior keys of the same type for the given account/device");

    keysManager.store(ACCOUNT_UUID, DEVICE_ID,
        List.of(generateTestPreKey(4), generateTestPreKey(5)),
        List.of(generateTestKEMSignedPreKey(6), generateTestKEMSignedPreKey(7)), null, generateTestKEMSignedPreKey(1002)).join();
    assertEquals(2, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Inserting multiple new keys should overwrite all prior keys for the given account/device");
    assertEquals(2, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join(),
        "Inserting multiple new keys should overwrite all prior keys for the given account/device");
    assertEquals(1002, keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID).join().get().keyId(),
        "Uploading new last-resort key should overwrite prior last-resort key for the account/device");
  }

  @Test
  void testTakeAccountAndDeviceId() {
    assertEquals(Optional.empty(), keysManager.takeEC(ACCOUNT_UUID, DEVICE_ID).join());

    final ECPreKey preKey = generateTestPreKey(1);

    keysManager.store(ACCOUNT_UUID, DEVICE_ID, List.of(preKey, generateTestPreKey(2)), null, null, null).join();
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

    keysManager.store(ACCOUNT_UUID, DEVICE_ID, null, List.of(preKey1, preKey2), null, preKeyLast).join();

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
  void testGetCount() {
    assertEquals(0, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertEquals(0, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());

    keysManager.store(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestPreKey(1)), List.of(generateTestKEMSignedPreKey(1)), null, null).join();
    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());
  }

  @Test
  void testDeleteByAccount() {
    keysManager.store(ACCOUNT_UUID, DEVICE_ID,
            List.of(generateTestPreKey(1), generateTestPreKey(2)),
            List.of(generateTestKEMSignedPreKey(3), generateTestKEMSignedPreKey(4)),
            generateTestECSignedPreKey(5),
            generateTestKEMSignedPreKey(6))
        .join();

    keysManager.store(ACCOUNT_UUID, DEVICE_ID + 1,
            List.of(generateTestPreKey(7)),
            List.of(generateTestKEMSignedPreKey(8)),
            generateTestECSignedPreKey(9),
            generateTestKEMSignedPreKey(10))
        .join();

    assertEquals(2, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertEquals(2, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertTrue(keysManager.getEcSignedPreKey(ACCOUNT_UUID, DEVICE_ID).join().isPresent());
    assertTrue(keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID).join().isPresent());
    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID + 1).join());
    assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID + 1).join());
    assertTrue(keysManager.getEcSignedPreKey(ACCOUNT_UUID, DEVICE_ID + 1).join().isPresent());
    assertTrue(keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID + 1).join().isPresent());

    keysManager.delete(ACCOUNT_UUID).join();

    assertEquals(0, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertEquals(0, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertFalse(keysManager.getEcSignedPreKey(ACCOUNT_UUID, DEVICE_ID).join().isPresent());
    assertFalse(keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID).join().isPresent());
    assertEquals(0, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID + 1).join());
    assertEquals(0, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID + 1).join());
    assertFalse(keysManager.getEcSignedPreKey(ACCOUNT_UUID, DEVICE_ID + 1).join().isPresent());
    assertFalse(keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID + 1).join().isPresent());
  }

  @Test
  void testDeleteByAccountAndDevice() {
    keysManager.store(ACCOUNT_UUID, DEVICE_ID,
            List.of(generateTestPreKey(1), generateTestPreKey(2)),
            List.of(generateTestKEMSignedPreKey(3), generateTestKEMSignedPreKey(4)),
            generateTestECSignedPreKey(5),
            generateTestKEMSignedPreKey(6))
        .join();

    keysManager.store(ACCOUNT_UUID, DEVICE_ID + 1,
            List.of(generateTestPreKey(7)),
            List.of(generateTestKEMSignedPreKey(8)),
            generateTestECSignedPreKey(9),
            generateTestKEMSignedPreKey(10))
        .join();

    assertEquals(2, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertEquals(2, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertTrue(keysManager.getEcSignedPreKey(ACCOUNT_UUID, DEVICE_ID).join().isPresent());
    assertTrue(keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID).join().isPresent());
    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID + 1).join());
    assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID + 1).join());
    assertTrue(keysManager.getEcSignedPreKey(ACCOUNT_UUID, DEVICE_ID + 1).join().isPresent());
    assertTrue(keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID + 1).join().isPresent());

    keysManager.delete(ACCOUNT_UUID, DEVICE_ID).join();

    assertEquals(0, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertEquals(0, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertFalse(keysManager.getEcSignedPreKey(ACCOUNT_UUID, DEVICE_ID).join().isPresent());
    assertFalse(keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID).join().isPresent());
    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID + 1).join());
    assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID + 1).join());
    assertTrue(keysManager.getEcSignedPreKey(ACCOUNT_UUID, DEVICE_ID + 1).join().isPresent());
    assertTrue(keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID + 1).join().isPresent());
  }

  @Test
  void testStorePqLastResort() {
    assertEquals(0, keysManager.getPqEnabledDevices(ACCOUNT_UUID).join().size());

    final ECKeyPair identityKeyPair = Curve.generateKeyPair();

    keysManager.storePqLastResort(
        ACCOUNT_UUID,
        Map.of(1L, KeysHelper.signedKEMPreKey(1, identityKeyPair), 2L, KeysHelper.signedKEMPreKey(2, identityKeyPair))).join();
    assertEquals(2, keysManager.getPqEnabledDevices(ACCOUNT_UUID).join().size());
    assertEquals(1L, keysManager.getLastResort(ACCOUNT_UUID, 1L).join().get().keyId());
    assertEquals(2L, keysManager.getLastResort(ACCOUNT_UUID, 2L).join().get().keyId());
    assertFalse(keysManager.getLastResort(ACCOUNT_UUID, 3L).join().isPresent());

    keysManager.storePqLastResort(
        ACCOUNT_UUID,
        Map.of(1L, KeysHelper.signedKEMPreKey(3, identityKeyPair), 3L, KeysHelper.signedKEMPreKey(4, identityKeyPair))).join();
    assertEquals(3, keysManager.getPqEnabledDevices(ACCOUNT_UUID).join().size(), "storing new last-resort keys should not create duplicates");
    assertEquals(3L, keysManager.getLastResort(ACCOUNT_UUID, 1L).join().get().keyId(), "storing new last-resort keys should overwrite old ones");
    assertEquals(2L, keysManager.getLastResort(ACCOUNT_UUID, 2L).join().get().keyId(), "storing new last-resort keys should leave untouched ones alone");
    assertEquals(4L, keysManager.getLastResort(ACCOUNT_UUID, 3L).join().get().keyId(), "storing new last-resort keys should overwrite old ones");
  }

  @Test
  void testGetPqEnabledDevices() {
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();

    keysManager.store(ACCOUNT_UUID, DEVICE_ID, null, List.of(KeysHelper.signedKEMPreKey(1, identityKeyPair)), null, null).join();
    keysManager.store(ACCOUNT_UUID, DEVICE_ID + 1, null, null, null, KeysHelper.signedKEMPreKey(2, identityKeyPair)).join();
    keysManager.store(ACCOUNT_UUID, DEVICE_ID + 2, null, List.of(KeysHelper.signedKEMPreKey(3, identityKeyPair)), null, KeysHelper.signedKEMPreKey(4, identityKeyPair)).join();
    keysManager.store(ACCOUNT_UUID, DEVICE_ID + 3, null, null, null, null).join();
    assertIterableEquals(
        Set.of(DEVICE_ID + 1, DEVICE_ID + 2),
        Set.copyOf(keysManager.getPqEnabledDevices(ACCOUNT_UUID).join()));
  }

  @Test
  void testStoreEcSignedPreKeyDisabled() {
    when(ecPreKeyMigrationConfiguration.storeEcSignedPreKeys()).thenReturn(false);

    final ECKeyPair identityKeyPair = Curve.generateKeyPair();

    keysManager.store(ACCOUNT_UUID, DEVICE_ID,
            List.of(generateTestPreKey(1)),
            List.of(KeysHelper.signedKEMPreKey(2, identityKeyPair)),
            KeysHelper.signedECPreKey(3, identityKeyPair),
            KeysHelper.signedKEMPreKey(4, identityKeyPair))
        .join();

    assertEquals(1, keysManager.getEcCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertEquals(1, keysManager.getPqCount(ACCOUNT_UUID, DEVICE_ID).join());
    assertTrue(keysManager.getLastResort(ACCOUNT_UUID, DEVICE_ID).join().isPresent());
    assertFalse(keysManager.getEcSignedPreKey(ACCOUNT_UUID, DEVICE_ID).join().isPresent());
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
