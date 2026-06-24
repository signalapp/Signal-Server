/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.whispersystems.textsecuregcm.util.AttributeValues.b;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

class DonationPermitsTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(DynamoDbExtensionSchema.Tables.DONATION_PERMITS);

  private static final Duration EXPIRATION = Duration.ofDays(7);

  private DonationPermits donationPermits;

  @BeforeEach
  void beforeEach() {

    donationPermits = new DonationPermits(
        DynamoDbExtensionSchema.Tables.DONATION_PERMITS.tableName(),
        EXPIRATION,
        DYNAMO_DB_EXTENSION.getDynamoDbClient());
  }

  @Test
  void testSpend() {

    final byte[] spendId1 = TestRandomUtil.nextBytes(16);

    final Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    assertTrue(donationPermits.spend(spendId1, now), "first spend should succeed");
    assertFalse(donationPermits.spend(spendId1, now), "second spend should fail");

    final GetItemResponse getItemResponse = DYNAMO_DB_EXTENSION.getDynamoDbClient()
        .getItem(GetItemRequest.builder()
            .tableName(DynamoDbExtensionSchema.Tables.DONATION_PERMITS.tableName())
            .key(Map.of(DonationPermits.KEY_SPEND_ID, b(spendId1)))
            .build());

    final Instant expiration = Instant.ofEpochSecond(
        Long.parseLong(getItemResponse.item().get(DonationPermits.KEY_EXPIRATION)
            .n()));

    assertEquals(now.plus(EXPIRATION), expiration);

    final byte[] spendId2 = TestRandomUtil.nextBytes(16);

    assertTrue(donationPermits.spend(spendId2, now), "spending a second ID should succeed");
  }

}
