/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

public class DeletedAccounts extends AbstractDynamoDbStore {

  // e164, primary key
  static final String KEY_ACCOUNT_E164 = "P";
  static final String ATTR_ACCOUNT_UUID = "U";
  static final String ATTR_EXPIRES = "E";

  static final String UUID_TO_E164_INDEX_NAME = "u_to_p";

  static final Duration TIME_TO_LIVE = Duration.ofDays(30);

  // Note that this limit is imposed by DynamoDB itself; going above 100 will result in errors
  static final int GET_BATCH_SIZE = 100;

  private final String tableName;

  public DeletedAccounts(final DynamoDbClient dynamoDb, final String tableName) {

    super(dynamoDb);
    this.tableName = tableName;
  }

  void put(UUID uuid, String e164) {
    db().putItem(PutItemRequest.builder()
        .tableName(tableName)
        .item(Map.of(
            KEY_ACCOUNT_E164, AttributeValues.fromString(e164),
            ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
            ATTR_EXPIRES, AttributeValues.fromLong(Instant.now().plus(TIME_TO_LIVE).getEpochSecond())))
        .build());
  }

  Optional<UUID> findUuid(final String e164) {
    final GetItemResponse response = db().getItem(GetItemRequest.builder()
        .tableName(tableName)
        .consistentRead(true)
        .key(Map.of(KEY_ACCOUNT_E164, AttributeValues.fromString(e164)))
        .build());

    return Optional.ofNullable(AttributeValues.getUUID(response.item(), ATTR_ACCOUNT_UUID, null));
  }

  Optional<String> findE164(final UUID uuid) {
    final QueryResponse response = db().query(QueryRequest.builder()
        .tableName(tableName)
        .indexName(UUID_TO_E164_INDEX_NAME)
        .keyConditionExpression("#uuid = :uuid")
        .projectionExpression("#e164")
        .expressionAttributeNames(Map.of("#uuid", ATTR_ACCOUNT_UUID,
            "#e164", KEY_ACCOUNT_E164))
        .expressionAttributeValues(Map.of(":uuid", AttributeValues.fromUUID(uuid))).build());

    if (response.count() == 0) {
      return Optional.empty();
    }

    if (response.count() > 1) {
      throw new RuntimeException(
          "Impossible result: more than one phone number returned for UUID: " + uuid);
    }

    return Optional.ofNullable(response.items().get(0).get(KEY_ACCOUNT_E164).s());
  }

  void remove(final String e164) {
    db().deleteItem(DeleteItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(KEY_ACCOUNT_E164, AttributeValues.fromString(e164)))
        .build());
  }
}
