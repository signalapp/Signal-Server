/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.Thread.State;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class DeletedAccountsManagerTest {

  private static final String NEEDS_RECONCILIATION_INDEX_NAME = "needs_reconciliation_test";

  @RegisterExtension
  static final DynamoDbExtension DELETED_ACCOUNTS_DYNAMODB_EXTENSION = DynamoDbExtension.builder()
      .tableName("deleted_accounts_test")
      .hashKey(DeletedAccounts.KEY_ACCOUNT_E164)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(DeletedAccounts.KEY_ACCOUNT_E164)
          .attributeType(ScalarAttributeType.S).build())
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(DeletedAccounts.ATTR_NEEDS_CDS_RECONCILIATION)
          .attributeType(ScalarAttributeType.N)
          .build())
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(DeletedAccounts.ATTR_ACCOUNT_UUID)
          .attributeType(ScalarAttributeType.B)
          .build())
      .globalSecondaryIndex(GlobalSecondaryIndex.builder()
          .indexName(NEEDS_RECONCILIATION_INDEX_NAME)
          .keySchema(
              KeySchemaElement.builder().attributeName(DeletedAccounts.KEY_ACCOUNT_E164).keyType(KeyType.HASH).build(),
              KeySchemaElement.builder().attributeName(DeletedAccounts.ATTR_NEEDS_CDS_RECONCILIATION)
                  .keyType(KeyType.RANGE).build())
          .projection(Projection.builder().projectionType(ProjectionType.INCLUDE)
              .nonKeyAttributes(DeletedAccounts.ATTR_ACCOUNT_UUID).build())
          .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build())
          .build())
      .globalSecondaryIndex(GlobalSecondaryIndex.builder()
          .indexName(DeletedAccounts.UUID_TO_E164_INDEX_NAME)
          .keySchema(
              KeySchemaElement.builder().attributeName(DeletedAccounts.ATTR_ACCOUNT_UUID).keyType(KeyType.HASH).build()
          )
          .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
          .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build())
          .build())
      .build();

  @RegisterExtension
  static DynamoDbExtension DELETED_ACCOUNTS_LOCK_DYNAMODB_EXTENSION = DynamoDbExtension.builder()
      .tableName("deleted_accounts_lock_test")
      .hashKey(DeletedAccounts.KEY_ACCOUNT_E164)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(DeletedAccounts.KEY_ACCOUNT_E164)
          .attributeType(ScalarAttributeType.S).build())
      .build();

  private DeletedAccounts deletedAccounts;
  private DeletedAccountsManager deletedAccountsManager;

  @BeforeEach
  void setUp() {
    deletedAccounts = new DeletedAccounts(DELETED_ACCOUNTS_DYNAMODB_EXTENSION.getDynamoDbClient(),
        DELETED_ACCOUNTS_DYNAMODB_EXTENSION.getTableName(),
        NEEDS_RECONCILIATION_INDEX_NAME);

    deletedAccountsManager = new DeletedAccountsManager(deletedAccounts,
        DELETED_ACCOUNTS_LOCK_DYNAMODB_EXTENSION.getLegacyDynamoClient(),
        DELETED_ACCOUNTS_LOCK_DYNAMODB_EXTENSION.getTableName());
  }

  @Test
  void testLockAndTake() throws InterruptedException {
    final UUID uuid = UUID.randomUUID();
    final String e164 = "+18005551234";

    deletedAccounts.put(uuid, e164, true);
    deletedAccountsManager.lockAndTake(e164, maybeUuid -> assertEquals(Optional.of(uuid), maybeUuid));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(e164));
  }

  @Test
  void testLockAndTakeWithException() {
    final UUID uuid = UUID.randomUUID();
    final String e164 = "+18005551234";

    deletedAccounts.put(uuid, e164, true);

    assertThrows(RuntimeException.class, () -> deletedAccountsManager.lockAndTake(e164, maybeUuid -> {
      assertEquals(Optional.of(uuid), maybeUuid);
      throw new RuntimeException("OH NO");
    }));

    assertEquals(Optional.of(uuid), deletedAccounts.findUuid(e164));
  }

  @Test
  void testReconciliationLockContention() throws ChunkProcessingFailedException {

    final UUID[] uuids = new UUID[3];
    final String[] e164s = new String[uuids.length];

    for (int i = 0; i < uuids.length; i++) {
      uuids[i] = UUID.randomUUID();
      e164s[i] = String.format("+1800555%04d", i);
    }

    final Map<String, UUID> expectedReconciledAccounts = new HashMap<>();

    for (int i = 0; i < uuids.length; i++) {
      deletedAccounts.put(uuids[i], e164s[i], true);
      expectedReconciledAccounts.put(e164s[i], uuids[i]);
    }

    final UUID replacedUUID = UUID.randomUUID();
    final Map<String, UUID> reconciledAccounts = new HashMap<>();

    final Thread putThread = new Thread(() -> {
      try {
        deletedAccountsManager.lockAndPut(e164s[0], () -> replacedUUID);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    },
        getClass().getSimpleName() + "-put");

    final Thread reconcileThread = new Thread(() -> {
      try {
        deletedAccountsManager.lockAndReconcileAccounts(uuids.length, deletedAccounts -> {
          // We hold the lock for the first account, so a thread trying to operate on that first count should block
          // waiting for the lock.
          putThread.start();

          // Make sure the other thread really does actually block at some point
          while (putThread.getState() != State.TIMED_WAITING) {
            Thread.yield();
          }

          deletedAccounts.forEach(pair -> reconciledAccounts.put(pair.second(), pair.first()));
          return reconciledAccounts.keySet();
        });
      } catch (ChunkProcessingFailedException e) {
        throw new AssertionError(e);
      }
    }, getClass().getSimpleName() + "-reconcile");

    reconcileThread.start();

    assertDoesNotThrow((Executable) reconcileThread::join);
    assertDoesNotThrow((Executable) putThread::join);

    assertEquals(expectedReconciledAccounts, reconciledAccounts);

    // The "put" thread should have completed after the reconciliation thread wrapped up. We can verify that's true by
    // reconciling again; the updated account (and only that account) should appear in the "needs reconciliation" list.
    deletedAccountsManager.lockAndReconcileAccounts(uuids.length, deletedAccounts -> {
      assertEquals(1, deletedAccounts.size());
      assertEquals(replacedUUID, deletedAccounts.get(0).first());
      assertEquals(e164s[0], deletedAccounts.get(0).second());

      return List.of(deletedAccounts.get(0).second());
    });
  }

}
