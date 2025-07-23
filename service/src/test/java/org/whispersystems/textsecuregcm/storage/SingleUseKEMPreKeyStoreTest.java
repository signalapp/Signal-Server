/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;
import java.util.Map;

class SingleUseKEMPreKeyStoreTest extends SingleUsePreKeyStoreTest<KEMSignedPreKey> {

  private SingleUseKEMPreKeyStore preKeyStore;

  private static final ECKeyPair IDENTITY_KEY_PAIR = ECKeyPair.generate();

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(DynamoDbExtensionSchema.Tables.PQ_KEYS);

  @BeforeEach
  void setUp() {
    preKeyStore = new SingleUseKEMPreKeyStore(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.PQ_KEYS.tableName());
  }

  @Override
  protected SingleUsePreKeyStore<KEMSignedPreKey> getPreKeyStore() {
    return preKeyStore;
  }

  @Override
  protected KEMSignedPreKey generatePreKey(final long keyId) {
    return KeysHelper.signedKEMPreKey(keyId, IDENTITY_KEY_PAIR);
  }

  @Override
  protected void clearKeyCountAttributes() {
    final ScanIterable scanIterable = DYNAMO_DB_EXTENSION.getDynamoDbClient().scanPaginator(ScanRequest.builder()
        .tableName(DynamoDbExtensionSchema.Tables.PQ_KEYS.tableName())
        .build());

    for (final ScanResponse response : scanIterable) {
      for (final Map<String, AttributeValue> item : response.items()) {

        DYNAMO_DB_EXTENSION.getDynamoDbClient().updateItem(UpdateItemRequest.builder()
            .tableName(DynamoDbExtensionSchema.Tables.PQ_KEYS.tableName())
            .key(Map.of(
                SingleUsePreKeyStore.KEY_ACCOUNT_UUID, item.get(SingleUsePreKeyStore.KEY_ACCOUNT_UUID),
                SingleUsePreKeyStore.KEY_DEVICE_ID_KEY_ID, item.get(SingleUsePreKeyStore.KEY_DEVICE_ID_KEY_ID)))
            .updateExpression("REMOVE " + SingleUsePreKeyStore.ATTR_REMAINING_KEYS)
            .build());
      }
    }
  }
}
