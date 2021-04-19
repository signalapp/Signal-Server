package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class MigrationRetryAccountsTest {

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
      .tableName("account_migration_errors_test")
      .hashKey(MigrationRetryAccounts.KEY_UUID)
      .attributeDefinition(new AttributeDefinition(MigrationRetryAccounts.KEY_UUID, ScalarAttributeType.B))
      .build();

  @Test
  void test() {

    final MigrationRetryAccounts migrationRetryAccounts = new MigrationRetryAccounts(dynamoDbExtension.getDynamoDB(),
        dynamoDbExtension.getTableName());

    UUID firstUuid = UUID.randomUUID();
    UUID secondUuid = UUID.randomUUID();

    assertTrue(migrationRetryAccounts.getUuids(10).isEmpty());

    migrationRetryAccounts.put(firstUuid);
    migrationRetryAccounts.put(secondUuid);

    assertTrue(migrationRetryAccounts.getUuids(10).containsAll(List.of(firstUuid, secondUuid)));
  }
}
