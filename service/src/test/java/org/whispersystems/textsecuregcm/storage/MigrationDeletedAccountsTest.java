package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class MigrationDeletedAccountsTest {

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
      .tableName("deleted_accounts_test")
      .hashKey(MigrationDeletedAccounts.KEY_UUID)
      .attributeDefinition(new AttributeDefinition(MigrationDeletedAccounts.KEY_UUID, ScalarAttributeType.B))
      .build();

  @Test
  void test() {

    final MigrationDeletedAccounts migrationDeletedAccounts = new MigrationDeletedAccounts(dynamoDbExtension.getDynamoDB(),
        dynamoDbExtension.getTableName());

    UUID firstUuid = UUID.randomUUID();
    UUID secondUuid = UUID.randomUUID();

    assertTrue(migrationDeletedAccounts.getRecentlyDeletedUuids().isEmpty());

    migrationDeletedAccounts.put(firstUuid);
    migrationDeletedAccounts.put(secondUuid);

    assertTrue(migrationDeletedAccounts.getRecentlyDeletedUuids().containsAll(List.of(firstUuid, secondUuid)));

    migrationDeletedAccounts.delete(List.of(firstUuid, secondUuid));

    assertTrue(migrationDeletedAccounts.getRecentlyDeletedUuids().isEmpty());
  }
}
