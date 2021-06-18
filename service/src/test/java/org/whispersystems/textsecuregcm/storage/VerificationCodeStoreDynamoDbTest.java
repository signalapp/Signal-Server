/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class VerificationCodeStoreDynamoDbTest extends VerificationCodeStoreTest {

  private VerificationCodeStoreDynamoDb verificationCodeStore;

  private static final String TABLE_NAME = "verification_code_test";

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = DynamoDbExtension.builder()
      .tableName(TABLE_NAME)
      .hashKey(VerificationCodeStoreDynamoDb.KEY_E164)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(VerificationCodeStoreDynamoDb.KEY_E164)
          .attributeType(ScalarAttributeType.S)
          .build())
      .build();

  @BeforeEach
  void setUp() {
    verificationCodeStore = new VerificationCodeStoreDynamoDb(DYNAMO_DB_EXTENSION.getDynamoDbClient(), TABLE_NAME);
  }

  @Override
  protected VerificationCodeStore getVerificationCodeStore() {
    return verificationCodeStore;
  }

  @Override
  protected boolean expectNullPushCode() {
    return false;
  }

  @Override
  protected boolean expectEmptyTwilioSid() {
    return false;
  }
}
