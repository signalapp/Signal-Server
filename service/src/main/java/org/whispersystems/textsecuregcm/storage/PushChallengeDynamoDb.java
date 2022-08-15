/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

/**
 * Stores push challenge tokens. Users may have at most one outstanding push challenge token at a time.
 */
public class PushChallengeDynamoDb extends AbstractDynamoDbStore {

  private final String tableName;
  private final Clock clock;

  static final String KEY_ACCOUNT_UUID = "U";
  static final String ATTR_CHALLENGE_TOKEN = "C";
  static final String ATTR_TTL = "T";

  private static final Map<String, String> UUID_NAME_MAP = Map.of("#uuid", KEY_ACCOUNT_UUID);
  private static final Map<String, String> CHALLENGE_TOKEN_NAME_MAP = Map.of("#challenge", ATTR_CHALLENGE_TOKEN, "#ttl",
      ATTR_TTL);

  public PushChallengeDynamoDb(final DynamoDbClient dynamoDB, final String tableName) {
    this(dynamoDB, tableName, Clock.systemUTC());
  }

  @VisibleForTesting
  PushChallengeDynamoDb(final DynamoDbClient dynamoDB, final String tableName, final Clock clock) {
    super(dynamoDB);

    this.tableName = tableName;
    this.clock = clock;
  }

  /**
   * Stores a push challenge token for the given user if and only if the user doesn't already have a token stored. The
   * existence check is strongly-consistent.
   *
   * @param accountUuid the UUID of the account for which to store a push challenge token
   * @param challengeToken the challenge token itself
   * @param ttl the time after which the token is no longer valid
   * @return {@code true} if a new token was stored of {@code false} if another token already exists for the given
   * account
   */
  public boolean add(final UUID accountUuid, final byte[] challengeToken, final Duration ttl) {
    try {
      db().putItem(PutItemRequest.builder()
          .tableName(tableName)
          .item(Map.of(
              KEY_ACCOUNT_UUID, AttributeValues.fromUUID(accountUuid),
              ATTR_CHALLENGE_TOKEN, AttributeValues.fromByteArray(challengeToken),
              ATTR_TTL, AttributeValues.fromLong(getExpirationTimestamp(ttl))))
          .conditionExpression("attribute_not_exists(#uuid)")
          .expressionAttributeNames(UUID_NAME_MAP)
          .build());
      return true;
    } catch (final ConditionalCheckFailedException e) {
      return false;
    }
  }

  long getExpirationTimestamp(final Duration ttl) {
    return clock.instant().plus(ttl).getEpochSecond();
  }

  /**
   * Clears a push challenge token for the given user if and only if the given challenge token matches the stored token.
   * The token comparison is a strongly-consistent operation.
   *
   * @param accountUuid the account for which to remove a stored token
   * @param challengeToken the token to remove
   * @return {@code true} if the given token matched the stored token for the given user or {@code false} otherwise
   */
  public boolean remove(final UUID accountUuid, final byte[] challengeToken) {
    try {
      db().deleteItem(DeleteItemRequest.builder()
          .tableName(tableName)
          .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(accountUuid)))
          .conditionExpression("#challenge = :challenge AND #ttl >= :currentTime")
          .expressionAttributeNames(CHALLENGE_TOKEN_NAME_MAP)
          .expressionAttributeValues(Map.of(":challenge", AttributeValues.fromByteArray(challengeToken),
              ":currentTime", AttributeValues.fromLong(clock.instant().getEpochSecond())))
          .build());
      return true;
    } catch (final ConditionalCheckFailedException e) {
      return false;
    }
  }
}
