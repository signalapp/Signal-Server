/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class PushChallengeDynamoDbTest {

  private PushChallengeDynamoDb pushChallengeDynamoDb;

  private static final long CURRENT_TIME_MILLIS = 1_000_000_000;

  private static final Random RANDOM = new Random();

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.PUSH_CHALLENGES);

  @BeforeEach
  void setUp() {
    this.pushChallengeDynamoDb = new PushChallengeDynamoDb(
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        Tables.PUSH_CHALLENGES.tableName(),
        Clock.fixed(Instant.ofEpochMilli(CURRENT_TIME_MILLIS), ZoneId.systemDefault()));
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
    assertTrue(pushChallengeDynamoDb.add(uuid, token, Duration.ofMinutes(-1)));
    assertFalse(pushChallengeDynamoDb.remove(uuid, token));
  }

  @Test
  void getExpirationTimestamp() {
    assertEquals((CURRENT_TIME_MILLIS / 1000) + 3600,
        pushChallengeDynamoDb.getExpirationTimestamp(Duration.ofHours(1)));
  }

  private static byte[] generateRandomToken() {
    return TestRandomUtil.nextBytes(16);
  }
}
