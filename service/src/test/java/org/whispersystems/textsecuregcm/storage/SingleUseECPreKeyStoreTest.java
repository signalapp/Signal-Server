/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

class SingleUseECPreKeyStoreTest {

  private static final int KEY_COUNT = 100;
  private SingleUseECPreKeyStore preKeyStore;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(DynamoDbExtensionSchema.Tables.EC_KEYS);

  @BeforeEach
  void setUp() {
    preKeyStore = new SingleUseECPreKeyStore(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.EC_KEYS.tableName());
  }

  private ECPreKey generatePreKey(final long keyId) {
    return new ECPreKey(keyId, ECKeyPair.generate().getPublicKey());
  }

  private void clearKeyCountAttributes() {
    final ScanIterable scanIterable = DYNAMO_DB_EXTENSION.getDynamoDbClient().scanPaginator(ScanRequest.builder()
        .tableName(DynamoDbExtensionSchema.Tables.EC_KEYS.tableName())
        .build());

    for (final ScanResponse response : scanIterable) {
      for (final Map<String, AttributeValue> item : response.items()) {

        DYNAMO_DB_EXTENSION.getDynamoDbClient().updateItem(UpdateItemRequest.builder()
            .tableName(DynamoDbExtensionSchema.Tables.EC_KEYS.tableName())
            .key(Map.of(
                SingleUseECPreKeyStore.KEY_ACCOUNT_UUID, item.get(SingleUseECPreKeyStore.KEY_ACCOUNT_UUID),
                SingleUseECPreKeyStore.KEY_DEVICE_ID_KEY_ID, item.get(SingleUseECPreKeyStore.KEY_DEVICE_ID_KEY_ID)))
            .updateExpression("REMOVE " + SingleUseECPreKeyStore.ATTR_REMAINING_KEYS)
            .build());
      }
    }
  }

  @Test
  void storeTake() {
    final SingleUseECPreKeyStore preKeyStore = this.preKeyStore;

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(Optional.empty(), preKeyStore.take(accountIdentifier, deviceId).join());

    final List<ECPreKey> sortedPreKeys;
    {
      final List<ECPreKey> preKeys = generateRandomPreKeys();
      assertDoesNotThrow(() -> preKeyStore.store(accountIdentifier, deviceId, preKeys).join());

      sortedPreKeys = new ArrayList<>(preKeys);
      sortedPreKeys.sort(Comparator.comparing(preKey -> preKey.keyId()));
    }

    assertEquals(Optional.of(sortedPreKeys.get(0)), preKeyStore.take(accountIdentifier, deviceId).join());
    assertEquals(Optional.of(sortedPreKeys.get(1)), preKeyStore.take(accountIdentifier, deviceId).join());
  }

  @Test
  void getCount() {
    final SingleUseECPreKeyStore preKeyStore = this.preKeyStore;

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());

    final List<ECPreKey> preKeys = generateRandomPreKeys();

    preKeyStore.store(accountIdentifier, deviceId, preKeys).join();

    assertEquals(KEY_COUNT, preKeyStore.getCount(accountIdentifier, deviceId).join());

    for (int i = 0; i < KEY_COUNT; i++) {
      preKeyStore.take(accountIdentifier, deviceId).join();
      assertEquals(KEY_COUNT - (i + 1), preKeyStore.getCount(accountIdentifier, deviceId).join());
    }

    preKeyStore.store(accountIdentifier, deviceId, List.of(generatePreKey(KEY_COUNT + 1))).join();
    clearKeyCountAttributes();

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());
  }

  @Test
  void deleteSingleDevice() {
    final SingleUseECPreKeyStore preKeyStore = this.preKeyStore;

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());
    assertDoesNotThrow(() -> preKeyStore.delete(accountIdentifier, deviceId).join());

    final List<ECPreKey> preKeys = generateRandomPreKeys();

    preKeyStore.store(accountIdentifier, deviceId, preKeys).join();
    preKeyStore.store(accountIdentifier, (byte) (deviceId + 1), preKeys).join();

    assertDoesNotThrow(() -> preKeyStore.delete(accountIdentifier, deviceId).join());

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());
    assertEquals(KEY_COUNT, preKeyStore.getCount(accountIdentifier, (byte) (deviceId + 1)).join());
  }

  @Test
  void deleteAllDevices() {
    final SingleUseECPreKeyStore preKeyStore = this.preKeyStore;

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());
    assertDoesNotThrow(() -> preKeyStore.delete(accountIdentifier).join());

    final List<ECPreKey> preKeys = generateRandomPreKeys();

    preKeyStore.store(accountIdentifier, deviceId, preKeys).join();
    preKeyStore.store(accountIdentifier, (byte) (deviceId + 1), preKeys).join();

    assertDoesNotThrow(() -> preKeyStore.delete(accountIdentifier).join());

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());
    assertEquals(0, preKeyStore.getCount(accountIdentifier, (byte) (deviceId + 1)).join());
  }

  private List<ECPreKey> generateRandomPreKeys() {
    final Set<Integer> keyIds = new HashSet<>(KEY_COUNT);

    while (keyIds.size() < KEY_COUNT) {
      keyIds.add(Math.abs(ThreadLocalRandom.current().nextInt()));
    }

    return keyIds.stream()
        .map(this::generatePreKey)
        .toList();
  }
}
