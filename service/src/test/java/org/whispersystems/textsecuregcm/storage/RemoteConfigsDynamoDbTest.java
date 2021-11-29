/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class RemoteConfigsDynamoDbTest extends RemoteConfigsTest {

  private static final String REMOTE_CONFIGS_TABLE_NAME = "remote_configs_test";

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
      .tableName(REMOTE_CONFIGS_TABLE_NAME)
      .hashKey(RemoteConfigsDynamoDb.KEY_NAME)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(RemoteConfigsDynamoDb.KEY_NAME)
          .attributeType(ScalarAttributeType.S)
          .build())
      .build();

  private RemoteConfigsDynamoDb remoteConfigs;

  @BeforeEach
  void setUp() {
    remoteConfigs = new RemoteConfigsDynamoDb(dynamoDbExtension.getDynamoDbClient(), REMOTE_CONFIGS_TABLE_NAME);
  }

  @Override
  protected RemoteConfigStore getRemoteConfigStore() {
    return remoteConfigs;
  }
}
