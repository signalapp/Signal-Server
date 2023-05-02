/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;

class DeletedAccountsTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.DELETED_ACCOUNTS);

  private DeletedAccounts deletedAccounts;

  @BeforeEach
  void setUp() {
    deletedAccounts = new DeletedAccounts(DYNAMO_DB_EXTENSION.getDynamoDbClient(), Tables.DELETED_ACCOUNTS.tableName());
  }

  @Test
  void testPutFind() {
    final UUID uuid = UUID.randomUUID();
    final String e164 = "+18005551234";

    assertEquals(Optional.empty(), deletedAccounts.findUuid(e164));

    deletedAccounts.put(uuid, e164);

    assertEquals(Optional.of(uuid), deletedAccounts.findUuid(e164));
  }

  @Test
  void testRemove() {
    final UUID uuid = UUID.randomUUID();
    final String e164 = "+18005551234";

    assertEquals(Optional.empty(), deletedAccounts.findUuid(e164));

    deletedAccounts.put(uuid, e164);

    assertEquals(Optional.of(uuid), deletedAccounts.findUuid(e164));

    deletedAccounts.remove(e164);

    assertEquals(Optional.empty(), deletedAccounts.findUuid(e164));
  }

  @Test
  void testFindE164() {
    assertEquals(Optional.empty(), deletedAccounts.findE164(UUID.randomUUID()));

    final UUID uuid = UUID.randomUUID();
    final String e164 = "+18005551234";

    deletedAccounts.put(uuid, e164);

    assertEquals(Optional.of(e164), deletedAccounts.findE164(uuid));
  }

  @Test
  void testFindUUID() {
    final String e164 = "+18005551234";

    assertEquals(Optional.empty(), deletedAccounts.findUuid(e164));

    final UUID uuid = UUID.randomUUID();

    deletedAccounts.put(uuid, e164);

    assertEquals(Optional.of(uuid), deletedAccounts.findUuid(e164));
  }
}
