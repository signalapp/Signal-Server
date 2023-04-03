/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;

class PhoneNumberIdentifiersTest {

  @RegisterExtension
  static DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.PNI);

  private PhoneNumberIdentifiers phoneNumberIdentifiers;

  @BeforeEach
  void setUp() {
    phoneNumberIdentifiers = new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        Tables.PNI.tableName());
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

  @Test
  void getPhoneNumber() {
    final String number = "+18005551234";

    assertFalse(phoneNumberIdentifiers.getPhoneNumber(UUID.randomUUID()).isPresent());

    final UUID pni = phoneNumberIdentifiers.getPhoneNumberIdentifier(number);
    assertEquals(Optional.of(number), phoneNumberIdentifiers.getPhoneNumber(pni));
  }
}
