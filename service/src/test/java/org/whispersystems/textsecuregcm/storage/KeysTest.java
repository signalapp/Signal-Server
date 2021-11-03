/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.entities.PreKey;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class KeysTest {

    private Keys keys;

    @ClassRule
    public static KeysDynamoDbRule dynamoDbRule = new KeysDynamoDbRule();

    private static final UUID ACCOUNT_UUID = UUID.randomUUID();
    private static final long DEVICE_ID = 1L;

    @Before
    public void setup() {
        keys = new Keys(dynamoDbRule.getDynamoDbClient(), KeysDynamoDbRule.TABLE_NAME);
    }

    @Test
    public void testStore() {
        assertEquals("Initial pre-key count for an account should be zero",
                0, keys.getCount(ACCOUNT_UUID, DEVICE_ID));

        keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(1, "public-key")));
        assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID));

        keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(1, "public-key")));
        assertEquals("Repeatedly storing same key should have no effect",
                1, keys.getCount(ACCOUNT_UUID, DEVICE_ID));

        keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(2, "different-public-key")));
        assertEquals("Inserting a new key should overwrite all prior keys for the given account/device",
                1, keys.getCount(ACCOUNT_UUID, DEVICE_ID));

        keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(3, "third-public-key"), new PreKey(4, "fourth-public-key")));
        assertEquals("Inserting multiple new keys should overwrite all prior keys for the given account/device",
                2, keys.getCount(ACCOUNT_UUID, DEVICE_ID));
    }

    @Test
    public void testTakeAccountAndDeviceId() {
        assertEquals(Optional.empty(), keys.take(ACCOUNT_UUID, DEVICE_ID));

        final PreKey preKey = new PreKey(1, "public-key");

        keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(preKey, new PreKey(2, "different-pre-key")));
        assertEquals(Optional.of(preKey), keys.take(ACCOUNT_UUID, DEVICE_ID));
        assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID));
    }

    @Test
    public void testGetCount() {
        assertEquals(0, keys.getCount(ACCOUNT_UUID, DEVICE_ID));

        keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(1, "public-key")));
        assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID));
    }

    @Test
    public void testDeleteByAccount() {
        keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(1, "public-key"), new PreKey(2, "different-public-key")));
        keys.store(ACCOUNT_UUID, DEVICE_ID + 1, List.of(new PreKey(3, "public-key-for-different-device")));

        assertEquals(2, keys.getCount(ACCOUNT_UUID, DEVICE_ID));
        assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID + 1));

        keys.delete(ACCOUNT_UUID);

        assertEquals(0, keys.getCount(ACCOUNT_UUID, DEVICE_ID));
        assertEquals(0, keys.getCount(ACCOUNT_UUID, DEVICE_ID + 1));
    }

    @Test
    public void testDeleteByAccountAndDevice() {
        keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(new PreKey(1, "public-key"), new PreKey(2, "different-public-key")));
        keys.store(ACCOUNT_UUID, DEVICE_ID + 1, List.of(new PreKey(3, "public-key-for-different-device")));

        assertEquals(2, keys.getCount(ACCOUNT_UUID, DEVICE_ID));
        assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID + 1));

        keys.delete(ACCOUNT_UUID, DEVICE_ID);

        assertEquals(0, keys.getCount(ACCOUNT_UUID, DEVICE_ID));
        assertEquals(1, keys.getCount(ACCOUNT_UUID, DEVICE_ID + 1));
    }

    @Test
    public void testSortKeyPrefix() {
      AttributeValue got = Keys.getSortKeyPrefix(123);
      assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 123}, got.b().asByteArray());
    }
}
