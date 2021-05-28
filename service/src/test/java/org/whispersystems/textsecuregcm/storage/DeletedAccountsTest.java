/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.util.Pair;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class DeletedAccountsTest {

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
      .tableName("deleted_accounts_test")
      .hashKey(DeletedAccounts.KEY_ACCOUNT_UUID)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(DeletedAccounts.KEY_ACCOUNT_UUID)
          .attributeType(ScalarAttributeType.B).build())
      .build();

  @Test
  void test() {

    final DeletedAccounts deletedAccounts = new DeletedAccounts(dynamoDbExtension.getDynamoDbClient(),
        dynamoDbExtension.getTableName());

    UUID firstUuid = UUID.randomUUID();
    UUID secondUuid = UUID.randomUUID();

    String firstNumber = "+14152221234";
    String secondNumber = "+14152225678";

    assertTrue(deletedAccounts.list(1).isEmpty());

    deletedAccounts.put(firstUuid, firstNumber);
    deletedAccounts.put(secondUuid, secondNumber);

    assertEquals(1, deletedAccounts.list(1).size());

    assertTrue(deletedAccounts.list(10).containsAll(
        List.of(
            new Pair<>(firstUuid, firstNumber),
            new Pair<>(secondUuid, secondNumber))));

    deletedAccounts.delete(List.of(firstUuid, secondUuid));

    assertTrue(deletedAccounts.list(10).isEmpty());
  }
}
