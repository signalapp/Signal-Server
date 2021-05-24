package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class MigrationRetryAccountsTest {

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
      .tableName("account_migration_errors_test")
      .hashKey(MigrationRetryAccounts.KEY_UUID)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(MigrationRetryAccounts.KEY_UUID)
          .attributeType(ScalarAttributeType.B)
          .build())
      .build();

  @Test
  void test() {

    final MigrationRetryAccounts migrationRetryAccounts = new MigrationRetryAccounts(dynamoDbExtension.getDynamoDbClient(),
        dynamoDbExtension.getTableName());

    UUID firstUuid = UUID.randomUUID();
    UUID secondUuid = UUID.randomUUID();

    assertTrue(migrationRetryAccounts.getUuids(10).isEmpty());

    migrationRetryAccounts.put(firstUuid);
    migrationRetryAccounts.put(secondUuid);

    assertTrue(migrationRetryAccounts.getUuids(10).containsAll(List.of(firstUuid, secondUuid)));
  }
}
