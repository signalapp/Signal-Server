/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

public class ProfileAvatars {

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;
  private final Duration expiration;
  private final Clock clock;

  // Identity that owns this profile; hash key; byte array
  @VisibleForTesting
  static final String KEY_IDENTITY = "I";

  // URL of the avatar; string
  @VisibleForTesting
  static final String ATTR_URL = "U";

  // Expiration timestamp, in seconds since the epoch, of the avatar; number
  @VisibleForTesting
  static final String ATTR_TTL = "E";

  public ProfileAvatars(final DynamoDbClient dynamoDbClient, final String tableName, final Duration expiration, final
      Clock clock) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
    this.expiration = expiration;
    this.clock = clock;
  }

  /// Sets the avatar URL for an identity
  ///
  /// @return the previous URL, if any
  public Optional<String> setAvatarUrl(final byte[] identity, final String url) {
    final PutItemResponse response = dynamoDbClient.putItem(PutItemRequest.builder()
        .tableName(tableName)
        .item(Map.of(KEY_IDENTITY, AttributeValues.fromByteArray(identity),
            ATTR_URL, AttributeValues.fromString(url),
            ATTR_TTL, AttributeValues.fromLong(clock.instant().plus(expiration).getEpochSecond())))
        .returnValues(ReturnValue.ALL_OLD)
        .build());

    return Optional.ofNullable(AttributeValues.getString(response.attributes(), ATTR_URL, null));
  }

  /// @return the avatar URL, if present
  public Optional<String> updateAvatarTtl(final byte[] identity) {

    final UpdateItemResponse response;
    try {
      response = dynamoDbClient.updateItem(UpdateItemRequest.builder()
          .tableName(tableName)
          .key(Map.of(KEY_IDENTITY, AttributeValues.fromByteArray(identity)))
          .conditionExpression("attribute_exists(#url)")
          .updateExpression("SET #ttl = :ttl")
          .expressionAttributeNames(Map.of(
              "#ttl", ATTR_TTL,
              "#url", ATTR_URL))
          .expressionAttributeValues(
              Map.of(":ttl", AttributeValues.fromLong(clock.instant().plus(expiration).getEpochSecond())))
          .returnValues(ReturnValue.ALL_OLD)
          .build());
    } catch (ConditionalCheckFailedException _) {
      return Optional.empty();
    }

    return Optional.ofNullable(AttributeValues.getString(response.attributes(), ATTR_URL, null));
  }

  /// @return the deleted avatar URL, if present
  public Optional<String> deleteAvatarUrl(final byte[] identity) {
    final DeleteItemResponse response = dynamoDbClient.deleteItem(DeleteItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(KEY_IDENTITY, AttributeValues.fromByteArray(identity)))
        .returnValues(ReturnValue.ALL_OLD)
        .build());

    if (!response.hasAttributes()) {
      return Optional.empty();
    }

    return Optional.ofNullable(response.attributes().get(ATTR_URL))
        .map(AttributeValue::s);
  }
}
