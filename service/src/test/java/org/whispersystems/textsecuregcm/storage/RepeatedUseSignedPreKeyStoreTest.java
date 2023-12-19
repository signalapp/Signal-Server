/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;

abstract class RepeatedUseSignedPreKeyStoreTest<K extends SignedPreKey<?>> {

  protected abstract RepeatedUseSignedPreKeyStore<K> getKeyStore();

  protected abstract K generateSignedPreKey();

  protected abstract DynamoDbClient getDynamoDbClient();

  @Test
  void storeFind() {
    final RepeatedUseSignedPreKeyStore<K> keys = getKeyStore();

    assertEquals(Optional.empty(), keys.find(UUID.randomUUID(), Device.PRIMARY_ID).join());

    final UUID identifier = UUID.randomUUID();
    final byte deviceId = 1;
    final K signedPreKey = generateSignedPreKey();

    assertDoesNotThrow(() -> keys.store(identifier, deviceId, signedPreKey).join());
    assertEquals(Optional.of(signedPreKey), keys.find(identifier, deviceId).join());
  }

  @Test
  void buildTransactWriteItemForInsertion() {
    final RepeatedUseSignedPreKeyStore<K> keys = getKeyStore();

    assertEquals(Optional.empty(), keys.find(UUID.randomUUID(), Device.PRIMARY_ID).join());

    final UUID identifier = UUID.randomUUID();
    final K signedPreKey = generateSignedPreKey();

    getDynamoDbClient().transactWriteItems(TransactWriteItemsRequest.builder()
        .transactItems(keys.buildTransactWriteItemForInsertion(identifier, Device.PRIMARY_ID, signedPreKey))
        .build());

    assertEquals(Optional.of(signedPreKey), keys.find(identifier, Device.PRIMARY_ID).join());
  }

  @Test
  void buildTransactWriteItemForDeletion() {
    final RepeatedUseSignedPreKeyStore<K> keys = getKeyStore();

    final UUID identifier = UUID.randomUUID();
    final byte deviceId2 = 2;
    final K retainedPreKey = generateSignedPreKey();

    keys.store(identifier, Device.PRIMARY_ID, generateSignedPreKey()).join();
    keys.store(identifier, deviceId2, retainedPreKey).join();

    getDynamoDbClient().transactWriteItems(TransactWriteItemsRequest.builder()
            .transactItems(keys.buildTransactWriteItemForDeletion(identifier, Device.PRIMARY_ID))
        .build());

    assertEquals(Optional.empty(), keys.find(identifier, Device.PRIMARY_ID).join());
    assertEquals(Optional.of(retainedPreKey), keys.find(identifier, deviceId2).join());
  }
}
