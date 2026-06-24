/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.util.AttributeValues.b;
import static org.whispersystems.textsecuregcm.util.AttributeValues.n;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class DonationPermits {

  public static final String KEY_SPEND_ID = "I";  // B  (HashKey)
  public static final String KEY_EXPIRATION = "E";  // N

  private final String table;
  private final Duration expiration;
  private final DynamoDbClient dynamoDbClient;

  public DonationPermits(String tableName, Duration expiration,
      DynamoDbClient dynamoDbClient) {
    this.table = tableName;
    this.expiration = expiration;
    this.dynamoDbClient = dynamoDbClient;
  }

  public boolean spend(final byte[] spendId, final Instant now) {
    final UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_SPEND_ID, b(spendId)))
        .conditionExpression("attribute_not_exists(#id)")
        .returnValues(ReturnValue.NONE)
        .updateExpression("SET #exp = :exp")
        .expressionAttributeNames(Map.of(
            "#id", KEY_SPEND_ID,
            "#exp", KEY_EXPIRATION))
        .expressionAttributeValues(Map.of(
            ":exp", n(now.plus(expiration).getEpochSecond())))
        .build();
    try {
      dynamoDbClient.updateItem(updateItemRequest);

      return true;
    } catch (final ConditionalCheckFailedException _) {
      return false;
    }
  }
}
