/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class KeysTest {

  private Keys keys;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.KEYS);

  private static final UUID ACCOUNT_UUID = UUID.randomUUID();
  private static final long DEVICE_ID = 1L;

  @BeforeEach
  void setup() {
    keys = new Keys(DYNAMO_DB_EXTENSION.getDynamoDbClient(), Tables.KEYS.tableName());
  }

  @Test
  void testStore() {
    assertEquals(0, keys.getCount(ACCOUNT_UUID, DEVICE_ID),
        "Initial pre-key count for an account should be zero");

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(1, "public-key")));
    assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID));

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(1, "public-key")));
    assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID),
        "Repeatedly storing same key should have no effect");

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(2, "different-public-key")));
    assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID),
        "Inserting a new key should overwrite all prior keys for the given account/device");

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(3, "third-public-key"), new PreKey(4, "fourth-public-key")));
    assertEquals(2, keys.getCount(ACCOUNT_UUID, DEVICE_ID),
        "Inserting multiple new keys should overwrite all prior keys for the given account/device");
  }

  @Test
  void testTakeAccountAndDeviceId() {
    assertEquals(Optional.empty(), keys.take(ACCOUNT_UUID, DEVICE_ID));

    final PreKey preKey = new PreKey(1, "public-key");

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(preKey, new PreKey(2, "different-pre-key")));
    assertEquals(Optional.of(preKey), keys.take(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID));
  }

  @Test
  void testGetCount() {
    assertEquals(0, keys.getCount(ACCOUNT_UUID, DEVICE_ID));

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(1, "public-key")));
    assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID));
  }

  @Test
  void testDeleteByAccount() {
    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(1, "public-key"), new PreKey(2, "different-public-key")));
    keys.store(ACCOUNT_UUID, DEVICE_ID + 1, List.of(new PreKey(3, "public-key-for-different-device")));

    assertEquals(2, keys.getCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID + 1));

    keys.delete(ACCOUNT_UUID);

    assertEquals(0, keys.getCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(0, keys.getCount(ACCOUNT_UUID, DEVICE_ID + 1));
  }

  @Test
  void testDeleteByAccountAndDevice() {
    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(1, "public-key"), new PreKey(2, "different-public-key")));
    keys.store(ACCOUNT_UUID, DEVICE_ID + 1, List.of(new PreKey(3, "public-key-for-different-device")));

    assertEquals(2, keys.getCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID + 1));

    keys.delete(ACCOUNT_UUID, DEVICE_ID);

    assertEquals(0, keys.getCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID + 1));
  }

  @Test
  void testSortKeyPrefix() {
    AttributeValue got = Keys.getSortKeyPrefix(123);
    assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 123}, got.b().asByteArray());
  }
}
