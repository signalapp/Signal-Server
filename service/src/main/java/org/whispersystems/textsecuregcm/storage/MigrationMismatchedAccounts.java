/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

public class MigrationMismatchedAccounts extends AbstractDynamoDbStore {

  static final String KEY_UUID = "U";
  static final String ATTR_TIMESTAMP = "T";

  @VisibleForTesting
  static final long MISMATCH_CHECK_DELAY_MILLIS = Duration.ofMinutes(1).toMillis();

  private final String tableName;
  private final Clock clock;

  public void put(UUID uuid) {
    final Map<String, AttributeValue> item = primaryKey(uuid);
    item.put("T", AttributeValues.fromLong(clock.millis()));
    db().putItem(PutItemRequest.builder()
        .tableName(tableName)
        .item(item)
        .build());
  }

  public MigrationMismatchedAccounts(DynamoDbClient dynamoDb, String tableName) {
    this(dynamoDb, tableName, Clock.systemUTC());
  }

  @VisibleForTesting
  MigrationMismatchedAccounts(DynamoDbClient dynamoDb, String tableName, final Clock clock) {
    super(dynamoDb);

    this.tableName = tableName;
    this.clock = clock;
  }

  /**
   * returns a list of UUIDs stored in the table that have passed {@link #MISMATCH_CHECK_DELAY_MILLIS}
   */
  public List<UUID> getUuids(int max) {

    final List<UUID> uuids = new ArrayList<>();

    final ScanIterable scanPaginator = db().scanPaginator(ScanRequest.builder()
        .tableName(tableName)
        .filterExpression("#timestamp <= :timestamp")
        .expressionAttributeNames(Map.of("#timestamp", ATTR_TIMESTAMP))
        .expressionAttributeValues(Map.of(":timestamp",
            AttributeValues.fromLong(clock.millis() - MISMATCH_CHECK_DELAY_MILLIS)))
        .build());

    for (ScanResponse response : scanPaginator) {

      for (Map<String, AttributeValue> item : response.items()) {
        uuids.add(AttributeValues.getUUID(item, KEY_UUID, null));

        if (uuids.size() >= max) {
          break;
        }
      }

      if (uuids.size() >= max) {
        break;
      }
    }

    return uuids;
  }

  @VisibleForTesting
  public static Map<String, AttributeValue> primaryKey(UUID uuid) {
    final HashMap<String, AttributeValue> item = new HashMap<>();
    item.put(KEY_UUID, AttributeValues.fromUUID(uuid));
    return item;
  }

  public void delete(final List<UUID> uuidsToDelete) {

    writeInBatches(uuidsToDelete, (uuids -> {

      final List<WriteRequest> deletes = uuids.stream()
          .map(uuid -> WriteRequest.builder().deleteRequest(
              DeleteRequest.builder().key(Map.of(KEY_UUID, AttributeValues.fromUUID(uuid))).build()).build())
          .collect(Collectors.toList());

      executeTableWriteItemsUntilComplete(Map.of(tableName, deletes));
    }));
  }
}
