/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;
import java.util.Map;

class SingleUseECPreKeyStoreTest extends SingleUsePreKeyStoreTest<ECPreKey> {

  private SingleUseECPreKeyStore preKeyStore;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(DynamoDbExtensionSchema.Tables.EC_KEYS);

  @BeforeEach
  void setUp() {
    preKeyStore = new SingleUseECPreKeyStore(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.EC_KEYS.tableName());
  }

  @Override
  protected SingleUsePreKeyStore<ECPreKey> getPreKeyStore() {
    return preKeyStore;
  }

  @Override
  protected ECPreKey generatePreKey(final long keyId) {
    return new ECPreKey(keyId, ECKeyPair.generate().getPublicKey());
  }

  @Override
  protected void clearKeyCountAttributes() {
    final ScanIterable scanIterable = DYNAMO_DB_EXTENSION.getDynamoDbClient().scanPaginator(ScanRequest.builder()
        .tableName(DynamoDbExtensionSchema.Tables.EC_KEYS.tableName())
        .build());

    for (final ScanResponse response : scanIterable) {
      for (final Map<String, AttributeValue> item : response.items()) {

        DYNAMO_DB_EXTENSION.getDynamoDbClient().updateItem(UpdateItemRequest.builder()
            .tableName(DynamoDbExtensionSchema.Tables.EC_KEYS.tableName())
            .key(Map.of(
                SingleUsePreKeyStore.KEY_ACCOUNT_UUID, item.get(SingleUsePreKeyStore.KEY_ACCOUNT_UUID),
                SingleUsePreKeyStore.KEY_DEVICE_ID_KEY_ID, item.get(SingleUsePreKeyStore.KEY_DEVICE_ID_KEY_ID)))
            .updateExpression("REMOVE " + SingleUsePreKeyStore.ATTR_REMAINING_KEYS)
            .build());
      }
    }
  }
}
