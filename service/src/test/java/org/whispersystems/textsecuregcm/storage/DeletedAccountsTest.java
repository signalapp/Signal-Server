/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.util.Pair;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class DeletedAccountsTest {

  private static final String NEEDS_RECONCILIATION_INDEX_NAME = "needs_reconciliation_test";

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
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

  private DeletedAccounts deletedAccounts;

  @BeforeEach
  void setUp() {
    deletedAccounts = new DeletedAccounts(dynamoDbExtension.getDynamoDbClient(),
        dynamoDbExtension.getTableName(),
        NEEDS_RECONCILIATION_INDEX_NAME);
  }

  @Test
  void testPutList() {
    UUID firstUuid = UUID.randomUUID();
    UUID secondUuid = UUID.randomUUID();
    UUID thirdUuid = UUID.randomUUID();

    String firstNumber = "+14152221234";
    String secondNumber = "+14152225678";
    String thirdNumber = "+14159998765";

    assertTrue(deletedAccounts.listAccountsToReconcile(1).isEmpty());

    deletedAccounts.put(firstUuid, firstNumber, true);
    deletedAccounts.put(secondUuid, secondNumber, true);
    deletedAccounts.put(thirdUuid, thirdNumber, true);

    assertEquals(1, deletedAccounts.listAccountsToReconcile(1).size());

    assertTrue(deletedAccounts.listAccountsToReconcile(10).containsAll(
        List.of(
            new Pair<>(firstUuid, firstNumber),
            new Pair<>(secondUuid, secondNumber))));

    deletedAccounts.markReconciled(List.of(firstNumber, secondNumber));

    assertEquals(List.of(new Pair<>(thirdUuid, thirdNumber)), deletedAccounts.listAccountsToReconcile(10));

    deletedAccounts.markReconciled(List.of(thirdNumber));

    assertTrue(deletedAccounts.listAccountsToReconcile(1).isEmpty());
  }

  @Test
  void testPutFind() {
    final UUID uuid = UUID.randomUUID();
    final String e164 = "+18005551234";

    assertEquals(Optional.empty(), deletedAccounts.findUuid(e164));

    deletedAccounts.put(uuid, e164, true);

    assertEquals(Optional.of(uuid), deletedAccounts.findUuid(e164));
  }

  @Test
  void testRemove() {
    final UUID uuid = UUID.randomUUID();
    final String e164 = "+18005551234";

    assertEquals(Optional.empty(), deletedAccounts.findUuid(e164));

    deletedAccounts.put(uuid, e164, true);

    assertEquals(Optional.of(uuid), deletedAccounts.findUuid(e164));

    deletedAccounts.remove(e164);

    assertEquals(Optional.empty(), deletedAccounts.findUuid(e164));
  }

  @Test
  void testGetAccountsNeedingReconciliation() {
    final UUID firstUuid = UUID.randomUUID();
    final UUID secondUuid = UUID.randomUUID();

    final String firstNumber = "+14152221234";
    final String secondNumber = "+14152225678";
    final String thirdNumber = "+14159998765";

    assertEquals(Collections.emptySet(),
        deletedAccounts.getAccountsNeedingReconciliation(List.of(firstNumber, secondNumber, thirdNumber)));

    deletedAccounts.put(firstUuid, firstNumber, true);
    deletedAccounts.put(secondUuid, secondNumber, true);

    assertEquals(Set.of(firstNumber, secondNumber),
        deletedAccounts.getAccountsNeedingReconciliation(List.of(firstNumber, secondNumber, thirdNumber)));
  }

  @Test
  void testGetAccountsNeedingReconciliationLargeBatch() {
    final int itemCount = (DeletedAccounts.GET_BATCH_SIZE * 3) + 1;

    final Set<String> expectedAccountsNeedingReconciliation = new HashSet<>(itemCount);

    for (int i = 0; i < itemCount; i++) {
      final String e164 = String.format("+18000555%04d", i);

      deletedAccounts.put(UUID.randomUUID(), e164, true);
      expectedAccountsNeedingReconciliation.add(e164);
    }

    final Set<String> accountsNeedingReconciliation =
        deletedAccounts.getAccountsNeedingReconciliation(expectedAccountsNeedingReconciliation);

    assertEquals(expectedAccountsNeedingReconciliation, accountsNeedingReconciliation);
  }


  @Test
  void testFindE164() {
    assertEquals(Optional.empty(), deletedAccounts.findE164(UUID.randomUUID()));

    final UUID uuid = UUID.randomUUID();
    final String e164 = "+18005551234";

    deletedAccounts.put(uuid, e164, true);

    assertEquals(Optional.of(e164), deletedAccounts.findE164(uuid));
  }

  @Test
  void testFindUUID() {
    final String e164 = "+18005551234";

    assertEquals(Optional.empty(), deletedAccounts.findUuid(e164));

    final UUID uuid = UUID.randomUUID();

    deletedAccounts.put(uuid, e164, true);

    assertEquals(Optional.of(uuid), deletedAccounts.findUuid(e164));
  }
}
