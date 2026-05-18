/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;

class ChangeNumberWaitingPeriodManagerTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION =
      new DynamoDbExtension(Tables.CHANGE_NUMBER_WAITING_PERIODS);

  private static final Duration WAITING_PERIOD = Duration.ofDays(7);

  private ChangeNumberWaitingPeriodManager changeNumberWaitingPeriodManager;

  @BeforeEach
  void setUp() {
    changeNumberWaitingPeriodManager = new ChangeNumberWaitingPeriodManager(
        new ChangeNumberWaitingPeriods(Tables.CHANGE_NUMBER_WAITING_PERIODS.tableName(),
            DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
            DYNAMO_DB_EXTENSION.getDynamoDbClient()),
        WAITING_PERIOD,
        Clock.systemUTC());
  }

  @Test
  void testNewAccount() throws Exception {
    final UUID aci = UUID.randomUUID();

    assertTrue(changeNumberWaitingPeriodManager.getWaitingPeriodRemaining(aci).isEmpty());

    changeNumberWaitingPeriodManager.handleAccountCreated(aci, Instant.now())
        .get(5, TimeUnit.SECONDS);

    assertTrue(changeNumberWaitingPeriodManager.getWaitingPeriodRemaining(aci).isPresent());
  }

  @Test
  void testOldAccount() throws Exception {
    final UUID aci = UUID.randomUUID();

    changeNumberWaitingPeriodManager.handleAccountCreated(aci,
        Instant.now().minus(WAITING_PERIOD).minus(Duration.ofHours(1)))
        .get(5, TimeUnit.SECONDS);

    assertTrue(changeNumberWaitingPeriodManager.getWaitingPeriodRemaining(aci).isEmpty());
  }
}
