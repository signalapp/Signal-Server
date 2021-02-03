/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class MessagesDynamoDbRule extends LocalDynamoDbRule {

  public static final String TABLE_NAME = "Signal_Messages_UnitTest";

  @Override
  protected void before() throws Throwable {
    super.before();
    DynamoDB dynamoDB = getDynamoDB();
    CreateTableRequest createTableRequest = new CreateTableRequest()
            .withTableName(TABLE_NAME)
            .withKeySchema(new KeySchemaElement("H", "HASH"),
                           new KeySchemaElement("S", "RANGE"))
            .withAttributeDefinitions(new AttributeDefinition("H", ScalarAttributeType.B),
                                      new AttributeDefinition("S", ScalarAttributeType.B),
                                      new AttributeDefinition("U", ScalarAttributeType.B))
            .withProvisionedThroughput(new ProvisionedThroughput(20L, 20L))
            .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("Message_UUID_Index")
                                                                .withKeySchema(new KeySchemaElement("H", "HASH"),
                                                                               new KeySchemaElement("U", "RANGE"))
                                                                .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY)));
    dynamoDB.createTable(createTableRequest);
  }

  @Override
  protected void after() {
    super.after();
  }
}
