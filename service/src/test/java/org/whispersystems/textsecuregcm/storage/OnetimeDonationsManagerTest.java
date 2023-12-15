/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class OnetimeDonationsManagerTest {
  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(DynamoDbExtensionSchema.Tables.ONETIME_DONATIONS);
  private OneTimeDonationsManager oneTimeDonationsManager;

  @BeforeEach
  void beforeEach() {
    oneTimeDonationsManager = new OneTimeDonationsManager(
        DynamoDbExtensionSchema.Tables.ONETIME_DONATIONS.tableName(),
        Duration.ofDays(90),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient());
  }

  @Test
  void testSetGetPaidAtTimestamp() {
    final String validPaymentIntentId = "abc";
    final Instant paidAt = Instant.ofEpochSecond(1_000_000);
    final Instant fallBackTimestamp = Instant.ofEpochSecond(2_000_000);
    oneTimeDonationsManager.putPaidAt(validPaymentIntentId, paidAt).join();

    assertThat(oneTimeDonationsManager.getPaidAt(validPaymentIntentId, fallBackTimestamp).join()).isEqualTo(paidAt);
    assertThat(oneTimeDonationsManager.getPaidAt("invalidPaymentId", fallBackTimestamp).join()).isEqualTo(fallBackTimestamp);
  }
}
