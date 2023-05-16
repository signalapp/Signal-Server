/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.Select;

class KeysTest {

  private Keys keys;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      Tables.EC_KEYS, Tables.PQ_KEYS, Tables.PQ_LAST_RESORT_KEYS);

  private static final UUID ACCOUNT_UUID = UUID.randomUUID();
  private static final long DEVICE_ID = 1L;

  @BeforeEach
  void setup() {
    keys = new Keys(
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        Tables.EC_KEYS.tableName(),
        Tables.PQ_KEYS.tableName(),
        Tables.PQ_LAST_RESORT_KEYS.tableName());
  }

  @Test
  void testStore() {
    assertEquals(0, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Initial pre-key count for an account should be zero");
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID),
        "Initial pre-key count for an account should be zero");
    assertFalse(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).isPresent(),
        "Initial last-resort pre-key for an account should be missing");

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(1, "public-key")));
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(1, "public-key")));
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Repeatedly storing same key should have no effect");

    keys.store(ACCOUNT_UUID, DEVICE_ID, null, List.of(new SignedPreKey(1, "pq-public-key", "sig")), null);
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Uploading new PQ prekeys should have no effect on EC prekeys");
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));

    keys.store(ACCOUNT_UUID, DEVICE_ID, null, null, new SignedPreKey(1001, "pq-last-resort-key", "sig"));
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Uploading new PQ last-resort prekey should have no effect on EC prekeys");
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID),
        "Uploading new PQ last-resort prekey should have no effect on one-time PQ prekeys");
    assertEquals(1001, keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).get().getKeyId());

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(2, "different-public-key")), null, null);
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Inserting a new key should overwrite all prior keys of the same type for the given account/device");
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID),
        "Uploading new EC prekeys should have no effect on PQ prekeys");

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(3, "third-public-key")), List.of(new SignedPreKey(2, "different-pq-public-key", "sig")), null);
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Inserting a new key should overwrite all prior keys of the same type for the given account/device");
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID),
        "Inserting a new key should overwrite all prior keys of the same type for the given account/device");

    keys.store(ACCOUNT_UUID, DEVICE_ID,
        List.of(new PreKey(4, "fourth-public-key"), new PreKey(5, "fifth-public-key")),
        List.of(new SignedPreKey(6, "sixth-pq-key", "sig"), new SignedPreKey(7, "seventh-pq-key", "sig")),
        new SignedPreKey(1002, "new-last-resort-key", "sig"));
    assertEquals(2, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Inserting multiple new keys should overwrite all prior keys for the given account/device");
    assertEquals(2, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID),
        "Inserting multiple new keys should overwrite all prior keys for the given account/device");
    assertEquals(1002, keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).get().getKeyId(),
        "Uploading new last-resort key should overwrite prior last-resort key for the account/device");
  }

  @Test
  void testTakeAccountAndDeviceId() {
    assertEquals(Optional.empty(), keys.takeEC(ACCOUNT_UUID, DEVICE_ID));

    final PreKey preKey = new PreKey(1, "public-key");

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(preKey, new PreKey(2, "different-pre-key")));
    assertEquals(Optional.of(preKey), keys.takeEC(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
  }

  @Test
  void testTakePQ() {
    assertEquals(Optional.empty(), keys.takeEC(ACCOUNT_UUID, DEVICE_ID));

    final SignedPreKey preKey1 = new SignedPreKey(1, "public-key", "sig");
    final SignedPreKey preKey2 = new SignedPreKey(2, "different-public-key", "sig");
    final SignedPreKey preKeyLast = new SignedPreKey(1001, "last-public-key", "sig");

    keys.store(ACCOUNT_UUID, DEVICE_ID, null, List.of(preKey1, preKey2), preKeyLast);

    assertEquals(Optional.of(preKey1), keys.takePQ(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));

    assertEquals(Optional.of(preKey2), keys.takePQ(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));

    assertEquals(Optional.of(preKeyLast), keys.takePQ(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));

    assertEquals(Optional.of(preKeyLast), keys.takePQ(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));
  }

  @Test
  void testGetCount() {
    assertEquals(0, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(1, "public-key")), List.of(new SignedPreKey(1, "public-pq-key", "sig")), null);
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));
  }

  @Test
  void testDeleteByAccount() {
    keys.store(ACCOUNT_UUID, DEVICE_ID,
        List.of(new PreKey(1, "public-key"), new PreKey(2, "different-public-key")),
        List.of(new SignedPreKey(3, "public-pq-key", "sig"), new SignedPreKey(4, "different-pq-key", "sig")),
        new SignedPreKey(5, "last-pq-key", "sig"));

    keys.store(ACCOUNT_UUID, DEVICE_ID + 1,
        List.of(new PreKey(6, "public-key-for-different-device")),
        List.of(new SignedPreKey(7, "public-pq-key-for-different-device", "sig")),
        new SignedPreKey(8, "last-pq-key-for-different-device", "sig"));

    assertEquals(2, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(2, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));
    assertTrue(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).isPresent());
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertTrue(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID + 1).isPresent());

    keys.delete(ACCOUNT_UUID);

    assertEquals(0, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));
    assertFalse(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).isPresent());
    assertEquals(0, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertFalse(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID + 1).isPresent());
  }

  @Test
  void testDeleteByAccountAndDevice() {
    keys.store(ACCOUNT_UUID, DEVICE_ID,
        List.of(new PreKey(1, "public-key"), new PreKey(2, "different-public-key")),
        List.of(new SignedPreKey(3, "public-pq-key", "sig"), new SignedPreKey(4, "different-pq-key", "sig")),
        new SignedPreKey(5, "last-pq-key", "sig"));

    keys.store(ACCOUNT_UUID, DEVICE_ID + 1,
        List.of(new PreKey(6, "public-key-for-different-device")),
        List.of(new SignedPreKey(7, "public-pq-key-for-different-device", "sig")),
        new SignedPreKey(8, "last-pq-key-for-different-device", "sig"));

    assertEquals(2, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(2, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));
    assertTrue(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).isPresent());
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertTrue(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID + 1).isPresent());

    keys.delete(ACCOUNT_UUID, DEVICE_ID);

    assertEquals(0, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));
    assertFalse(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).isPresent());
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertTrue(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID + 1).isPresent());
  }

  @Test
  void testStorePqLastResort() {
    assertEquals(0, getLastResortCount(ACCOUNT_UUID));

    keys.storePqLastResort(
        ACCOUNT_UUID,
        Map.of(1L, new SignedPreKey(1L, "pub1", "sig1"), 2L, new SignedPreKey(2L, "pub2", "sig2")));
    assertEquals(2, getLastResortCount(ACCOUNT_UUID));
    assertEquals(1L, keys.getLastResort(ACCOUNT_UUID, 1L).get().getKeyId());
    assertEquals(2L, keys.getLastResort(ACCOUNT_UUID, 2L).get().getKeyId());
    assertFalse(keys.getLastResort(ACCOUNT_UUID, 3L).isPresent());

    keys.storePqLastResort(
        ACCOUNT_UUID,
        Map.of(1L, new SignedPreKey(3L, "pub3", "sig3"), 3L, new SignedPreKey(4L, "pub4", "sig4")));
    assertEquals(3, getLastResortCount(ACCOUNT_UUID), "storing new last-resort keys should not create duplicates");
    assertEquals(3L, keys.getLastResort(ACCOUNT_UUID, 1L).get().getKeyId(), "storing new last-resort keys should overwrite old ones");
    assertEquals(2L, keys.getLastResort(ACCOUNT_UUID, 2L).get().getKeyId(), "storing new last-resort keys should leave untouched ones alone");
    assertEquals(4L, keys.getLastResort(ACCOUNT_UUID, 3L).get().getKeyId(), "storing new last-resort keys should overwrite old ones");
  }

  private int getLastResortCount(UUID uuid) {
    QueryRequest queryRequest = QueryRequest.builder()
        .tableName(Tables.PQ_LAST_RESORT_KEYS.tableName())
        .keyConditionExpression("#uuid = :uuid")
        .expressionAttributeNames(Map.of("#uuid", Keys.KEY_ACCOUNT_UUID))
        .expressionAttributeValues(Map.of(":uuid", AttributeValues.fromUUID(uuid)))
        .select(Select.COUNT)
        .build();
    QueryResponse response = DYNAMO_DB_EXTENSION.getDynamoDbClient().query(queryRequest);
    return response.count();
  }

  @Test
  void testGetPqEnabledDevices() {
    keys.store(ACCOUNT_UUID, DEVICE_ID, null, List.of(new SignedPreKey(1L, "pub1", "sig1")), null);
    keys.store(ACCOUNT_UUID, DEVICE_ID + 1, null, null, new SignedPreKey(2L, "pub2", "sig2"));
    keys.store(ACCOUNT_UUID, DEVICE_ID + 2, null, List.of(new SignedPreKey(3L, "pub3", "sig3")), new SignedPreKey(4L, "pub4", "sig4"));
    keys.store(ACCOUNT_UUID, DEVICE_ID + 3, null, null, null);
    assertIterableEquals(
        Set.of(DEVICE_ID + 1, DEVICE_ID + 2),
        Set.copyOf(keys.getPqEnabledDevices(ACCOUNT_UUID)));
  }

  @Test
  void testSortKeyPrefix() {
    AttributeValue got = Keys.getSortKeyPrefix(123);
    assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 123}, got.b().asByteArray());
  }
}
