/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.whispersystems.textsecuregcm.tests.util.LocalDynamoDbRule;

public class KeysDynamoDbRule extends LocalDynamoDbRule {
    public static final String TABLE_NAME = "Signal_Keys_Test";

    @Override
    protected void before() throws Throwable {
        super.before();

        final DynamoDB dynamoDB = getDynamoDB();

        final CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName(TABLE_NAME)
                .withKeySchema(new KeySchemaElement(KeysDynamoDb.KEY_ACCOUNT_UUID, "HASH"),
                               new KeySchemaElement(KeysDynamoDb.KEY_DEVICE_ID_KEY_ID, "RANGE"))
                .withAttributeDefinitions(new AttributeDefinition(KeysDynamoDb.KEY_ACCOUNT_UUID, ScalarAttributeType.B),
                                          new AttributeDefinition(KeysDynamoDb.KEY_DEVICE_ID_KEY_ID, ScalarAttributeType.B))
                .withProvisionedThroughput(new ProvisionedThroughput(20L, 20L));

        dynamoDB.createTable(createTableRequest);
    }

    @Override
    protected void after() {
        super.after();
    }
}
