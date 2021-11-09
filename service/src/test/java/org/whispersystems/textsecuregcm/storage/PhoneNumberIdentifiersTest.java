/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class PhoneNumberIdentifiersTest {

  private static final String PNI_TABLE_NAME = "pni_test";

  @RegisterExtension
  static DynamoDbExtension DYNAMO_DB_EXTENSION = DynamoDbExtension.builder()
      .tableName(PNI_TABLE_NAME)
      .hashKey(PhoneNumberIdentifiers.KEY_E164)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(PhoneNumberIdentifiers.KEY_E164)
          .attributeType(ScalarAttributeType.S)
          .build())
      .build();

  private PhoneNumberIdentifiers phoneNumberIdentifiers;

  @BeforeEach
  void setUp() {
    phoneNumberIdentifiers = new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbClient(), PNI_TABLE_NAME);
  }

  @Test
  void getPhoneNumberIdentifier() {
    final String number = "+18005551234";
    final String differentNumber = "+18005556789";

    final UUID firstPni = phoneNumberIdentifiers.getPhoneNumberIdentifier(number);
    final UUID secondPni = phoneNumberIdentifiers.getPhoneNumberIdentifier(number);

    assertEquals(firstPni, secondPni);
    assertNotEquals(firstPni, phoneNumberIdentifiers.getPhoneNumberIdentifier(differentNumber));
  }

  @Test
  void generatePhoneNumberIdentifierIfNotExists() {
    final String number = "+18005551234";

    assertEquals(phoneNumberIdentifiers.generatePhoneNumberIdentifierIfNotExists(number),
        phoneNumberIdentifiers.generatePhoneNumberIdentifierIfNotExists(number));
  }
}
