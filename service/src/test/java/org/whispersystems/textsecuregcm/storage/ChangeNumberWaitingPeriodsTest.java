/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;

class ChangeNumberWaitingPeriodsTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION =
      new DynamoDbExtension(Tables.CHANGE_NUMBER_WAITING_PERIODS);

  private ChangeNumberWaitingPeriods changeNumberWaitingPeriods;

  @BeforeEach
  void setUp() {
    changeNumberWaitingPeriods = new ChangeNumberWaitingPeriods(
        Tables.CHANGE_NUMBER_WAITING_PERIODS.tableName(),
        DYNAMO_DB_EXTENSION.getDynamoDbClient());
  }

  @Test
  void getExpiration_unknownAci() {
    assertTrue(changeNumberWaitingPeriods.getExpiration(UUID.randomUUID()).isEmpty());
  }

  @Test
  void setAndGetExpiration() {
    final UUID aci = UUID.randomUUID();
    // truncate to seconds because the TTL attribute stores epoch seconds
    final Instant expiration = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.SECONDS);

    changeNumberWaitingPeriods.setExpiration(aci, expiration);

    final Optional<Instant> result = changeNumberWaitingPeriods.getExpiration(aci);
    assertTrue(result.isPresent());
    assertEquals(expiration, result.get());

    assertTrue(changeNumberWaitingPeriods.getExpiration(UUID.randomUUID()).isEmpty(), "new UUID has no entry");
  }

  @Test
  void setExpiration_overwritesExistingEntry() {
    final UUID aci = UUID.randomUUID();
    final Instant first = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.SECONDS);
    final Instant second = Instant.now().plusSeconds(7200).truncatedTo(ChronoUnit.SECONDS);

    changeNumberWaitingPeriods.setExpiration(aci, first);

    {
      final Optional<Instant> result = changeNumberWaitingPeriods.getExpiration(aci);
      assertTrue(result.isPresent());
      assertEquals(first, result.get());
    }

    changeNumberWaitingPeriods.setExpiration(aci, second);

    {
      final Optional<Instant> result = changeNumberWaitingPeriods.getExpiration(aci);
      assertTrue(result.isPresent());
      assertEquals(second, result.get());
    }
  }

  @Test
  void delete_removesExisting() {
    final UUID aci = UUID.randomUUID();
    changeNumberWaitingPeriods.setExpiration(aci, Instant.now().plusSeconds(3600));

    assertTrue( changeNumberWaitingPeriods.getExpiration(aci).isPresent());

    changeNumberWaitingPeriods.delete(aci);

    assertTrue(changeNumberWaitingPeriods.getExpiration(aci).isEmpty());
  }

  @Test
  void delete_missingIsNoop() {
    assertDoesNotThrow(() -> changeNumberWaitingPeriods.delete(UUID.randomUUID()));
  }
}
