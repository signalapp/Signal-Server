/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class MigrationMismatchAccountsTest {

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
      .tableName("account_migration_mismatches_test")
      .hashKey(MigrationRetryAccounts.KEY_UUID)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(MigrationRetryAccounts.KEY_UUID)
          .attributeType(ScalarAttributeType.B)
          .build())
      .build();

  @Test
  void test() {

    final Clock clock = mock(Clock.class);
    when(clock.millis()).thenReturn(0L);

    final MigrationMismatchedAccounts migrationMismatchedAccounts = new MigrationMismatchedAccounts(
        dynamoDbExtension.getDynamoDbClient(),
        dynamoDbExtension.getTableName(), clock);

    UUID firstUuid = UUID.randomUUID();
    UUID secondUuid = UUID.randomUUID();

    assertTrue(migrationMismatchedAccounts.getUuids(10).isEmpty());

    migrationMismatchedAccounts.put(firstUuid);
    migrationMismatchedAccounts.put(secondUuid);

    assertTrue(migrationMismatchedAccounts.getUuids(10).isEmpty());

    when(clock.millis()).thenReturn(MigrationMismatchedAccounts.MISMATCH_CHECK_DELAY_MILLIS);

    assertTrue(migrationMismatchedAccounts.getUuids(10).containsAll(List.of(firstUuid, secondUuid)));
  }
}
