/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;

class DeletedAccountsManagerTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION =
      new DynamoDbExtension(Tables.DELETED_ACCOUNTS, Tables.DELETED_ACCOUNTS_LOCK);

  private DeletedAccounts deletedAccounts;
  private DeletedAccountsManager deletedAccountsManager;

  @BeforeEach
  void setUp() {
    deletedAccounts = new DeletedAccounts(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        Tables.DELETED_ACCOUNTS.tableName());

    deletedAccountsManager = new DeletedAccountsManager(deletedAccounts,
        DYNAMO_DB_EXTENSION.getLegacyDynamoClient(),
        Tables.DELETED_ACCOUNTS_LOCK.tableName());
  }

  @Test
  void testLockAndTake() throws InterruptedException {
    final UUID uuid = UUID.randomUUID();
    final String e164 = "+18005551234";

    deletedAccounts.put(uuid, e164);
    deletedAccountsManager.lockAndTake(e164, maybeUuid -> assertEquals(Optional.of(uuid), maybeUuid));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(e164));
  }

  @Test
  void testLockAndTakeWithException() {
    final UUID uuid = UUID.randomUUID();
    final String e164 = "+18005551234";

    deletedAccounts.put(uuid, e164);

    assertThrows(RuntimeException.class, () -> deletedAccountsManager.lockAndTake(e164, maybeUuid -> {
      assertEquals(Optional.of(uuid), maybeUuid);
      throw new RuntimeException("OH NO");
    }));

    assertEquals(Optional.of(uuid), deletedAccounts.findUuid(e164));
  }
}
