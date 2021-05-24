/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

public class MessagesDynamoDbRule extends LocalDynamoDbRule {

  public static final String TABLE_NAME = "Signal_Messages_UnitTest";

  @Override
  protected void before() throws Throwable {
    super.before();
    getDynamoDbClient().createTable(CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(KeySchemaElement.builder().attributeName("H").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("S").keyType(KeyType.RANGE).build())
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName("H").attributeType(ScalarAttributeType.B).build(),
            AttributeDefinition.builder().attributeName("S").attributeType(ScalarAttributeType.B).build(),
            AttributeDefinition.builder().attributeName("U").attributeType(ScalarAttributeType.B).build())
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(20L).writeCapacityUnits(20L).build())
        .localSecondaryIndexes(LocalSecondaryIndex.builder().indexName("Message_UUID_Index")
            .keySchema(KeySchemaElement.builder().attributeName("H").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName("U").keyType(KeyType.RANGE).build())
            .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
            .build())
        .build());
  }

  @Override
  protected void after() {
    super.after();
  }
}
