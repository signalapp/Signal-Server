/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.Pair;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DeletedAccounts extends AbstractDynamoDbStore {

  // uuid, primary key
  static final String KEY_ACCOUNT_UUID = "U";
  static final String ATTR_ACCOUNT_E164 = "P";

  private final String tableName;

  public DeletedAccounts(final DynamoDbClient dynamoDb, final String tableName) {

    super(dynamoDb);
    this.tableName = tableName;
  }

  public void put(UUID uuid, String e164) {
    db().putItem(PutItemRequest.builder()
        .tableName(tableName)
        .item(Map.of(
            KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
            ATTR_ACCOUNT_E164, AttributeValues.fromString(e164)))
        .build());
  }

  public List<Pair<UUID, String>> list(final int max) {

    final ScanRequest scanRequest = ScanRequest.builder()
        .tableName(tableName)
        .limit(max)
        .build();

    return scan(scanRequest, max)
        .stream()
        .map(item -> new Pair<>(
            AttributeValues.getUUID(item, KEY_ACCOUNT_UUID, null),
            AttributeValues.getString(item, ATTR_ACCOUNT_E164, null)))
        .collect(Collectors.toList());
  }

  public void delete(final List<UUID> uuidsToDelete) {

    writeInBatches(uuidsToDelete, (uuids -> {

      final List<WriteRequest> deletes = uuids.stream()
          .map(uuid -> WriteRequest.builder().deleteRequest(
              DeleteRequest.builder().key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid))).build()).build())
          .collect(Collectors.toList());

      executeTableWriteItemsUntilComplete(Map.of(tableName, deletes));
    }));
  }

}
