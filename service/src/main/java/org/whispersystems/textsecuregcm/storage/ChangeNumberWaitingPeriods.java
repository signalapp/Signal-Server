/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

public class ChangeNumberWaitingPeriods {

  // hash key; bytes
  static final String KEY_ACCOUNT_UUID = "U";
  // expiration timestamp in epoch seconds; number
  static final String ATTR_TTL = "E";

  private final String tableName;
  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final DynamoDbClient dynamoDbClient;

  public ChangeNumberWaitingPeriods(final String tableName, final DynamoDbAsyncClient dynamoDbAsyncClient, final DynamoDbClient dynamoDbClient) {
    this.tableName = tableName;
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.dynamoDbClient = dynamoDbClient;
  }

  public CompletableFuture<Void> setExpiration(final UUID aci, final Instant expiration) {
    return dynamoDbAsyncClient.putItem(PutItemRequest.builder()
            .tableName(tableName)
            .item(Map.of(
                KEY_ACCOUNT_UUID, AttributeValues.fromUUID(aci),
                ATTR_TTL, AttributeValues.fromLong(expiration.getEpochSecond())))
            .build())
        .thenRun(Util.NOOP);
  }

  public Optional<Instant> getExpiration(final UUID aci) {
    final GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(aci)))
            .consistentRead(true)
            .build());
    if (!response.hasItem()) {
      return Optional.empty();
    }

    return AttributeValues.get(response.item(), ATTR_TTL)
        .map(AttributeValue::n)
        .map(Long::parseLong)
        .map(Instant::ofEpochSecond)
        .filter(instant -> instant.isAfter(Instant.now()));
  }

  @VisibleForTesting
  public void delete(final UUID aci) {
    dynamoDbClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(aci)))
            .build());
  }
}
