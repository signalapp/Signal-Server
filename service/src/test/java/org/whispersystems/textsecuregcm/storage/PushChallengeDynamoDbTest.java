/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class PushChallengeDynamoDbTest {

  private PushChallengeDynamoDb pushChallengeDynamoDb;

  private static final long CURRENT_TIME_MILLIS = 1_000_000_000;

  private static final Random RANDOM = new Random();
  private static final String TABLE_NAME = "push_challenge_test";

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
      .tableName(TABLE_NAME)
      .hashKey(PushChallengeDynamoDb.KEY_ACCOUNT_UUID)
      .attributeDefinition(new AttributeDefinition(PushChallengeDynamoDb.KEY_ACCOUNT_UUID, ScalarAttributeType.B))
      .build();

  @BeforeEach
  void setUp() {
    this.pushChallengeDynamoDb = new PushChallengeDynamoDb(dynamoDbExtension.getDynamoDB(), TABLE_NAME, Clock.fixed(
        Instant.ofEpochMilli(CURRENT_TIME_MILLIS), ZoneId.systemDefault()));
  }

  @Test
  void add() {
    final UUID uuid = UUID.randomUUID();

    assertTrue(pushChallengeDynamoDb.add(uuid, generateRandomToken(), Duration.ofMinutes(1)));
    assertFalse(pushChallengeDynamoDb.add(uuid, generateRandomToken(), Duration.ofMinutes(1)));
  }

  @Test
  void remove() {
    final UUID uuid = UUID.randomUUID();
    final byte[] token = generateRandomToken();

    assertFalse(pushChallengeDynamoDb.remove(uuid, token));
    assertTrue(pushChallengeDynamoDb.add(uuid, token, Duration.ofMinutes(1)));
    assertTrue(pushChallengeDynamoDb.remove(uuid, token));
  }

  @Test
  void getExpirationTimestamp() {
    assertEquals((CURRENT_TIME_MILLIS / 1000) + 3600,
        pushChallengeDynamoDb.getExpirationTimestamp(Duration.ofHours(1)));
  }

  private static byte[] generateRandomToken() {
    final byte[] token = new byte[16];
    RANDOM.nextBytes(token);

    return token;
  }
}
