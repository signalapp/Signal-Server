/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.tests.util.LocalDynamoDbRule;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

public class KeysDynamoDbRule extends LocalDynamoDbRule {
    public static final String TABLE_NAME = "Signal_Keys_Test";

    @Override
    protected void before() throws Throwable {
        super.before();
      getDynamoDbClient().createTable(CreateTableRequest.builder()
          .tableName(TABLE_NAME)
          .keySchema(
              KeySchemaElement.builder().attributeName(KeysDynamoDb.KEY_ACCOUNT_UUID).keyType(KeyType.HASH).build(),
              KeySchemaElement.builder().attributeName(KeysDynamoDb.KEY_DEVICE_ID_KEY_ID).keyType(KeyType.RANGE)
                  .build())
          .attributeDefinitions(AttributeDefinition.builder()
                  .attributeName(KeysDynamoDb.KEY_ACCOUNT_UUID)
                  .attributeType(ScalarAttributeType.B)
                  .build(),
              AttributeDefinition.builder()
                  .attributeName(KeysDynamoDb.KEY_DEVICE_ID_KEY_ID)
                  .attributeType(ScalarAttributeType.B)
                  .build())
          .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(20L).writeCapacityUnits(20L).build())
          .build());
    }

    @Override
    protected void after() {
        super.after();
    }
}
