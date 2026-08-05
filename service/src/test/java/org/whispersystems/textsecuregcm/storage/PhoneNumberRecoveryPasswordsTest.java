/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

class PhoneNumberRecoveryPasswordsTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.REGISTRATION_RECOVERY_PASSWORDS
  );

  private PhoneNumberRecoveryPasswords phoneNumberRecoveryPasswords;

  @BeforeEach
  void setUp() {
    phoneNumberRecoveryPasswords = new PhoneNumberRecoveryPasswords(
        DynamoDbExtensionSchema.Tables.REGISTRATION_RECOVERY_PASSWORDS.tableName(),
        Duration.ofDays(1),
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        Clock.systemUTC());
  }

  @Test
  void buildConditionCheckForMigration() {
    final UUID phoneNumberIdentifier = UUID.randomUUID();

    final SaltedTokenHash originalPassword =
        SaltedTokenHash.generateFor(RandomStringUtils.insecure().nextAlphanumeric(16));

    phoneNumberRecoveryPasswords.addOrReplace(phoneNumberIdentifier, originalPassword);

    final TransactWriteItem transactWriteItem =
        phoneNumberRecoveryPasswords.buildConditionCheckForMigration(phoneNumberIdentifier, originalPassword);

    assertDoesNotThrow(() -> DYNAMO_DB_EXTENSION.getDynamoDbClient().transactWriteItems(TransactWriteItemsRequest.builder()
        .transactItems(transactWriteItem)
        .build()));

    final SaltedTokenHash changedPassword =
        new SaltedTokenHash(originalPassword.salt() + "-different", originalPassword.hash() + "-different");

    phoneNumberRecoveryPasswords.addOrReplace(phoneNumberIdentifier, changedPassword);

    assertThrows(TransactionCanceledException.class, () ->
        DYNAMO_DB_EXTENSION.getDynamoDbClient().transactWriteItems(TransactWriteItemsRequest.builder()
            .transactItems(transactWriteItem)
            .build()),
        "Transaction should not proceed if password has changed");

    phoneNumberRecoveryPasswords.removeEntry(phoneNumberIdentifier);

    assertThrows(TransactionCanceledException.class, () ->
        DYNAMO_DB_EXTENSION.getDynamoDbClient().transactWriteItems(TransactWriteItemsRequest.builder()
            .transactItems(transactWriteItem)
            .build()),
        "Transaction should not proceed if password has been removed");
  }
}
