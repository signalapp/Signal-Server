/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class VerificationCodeStoreTest {

  private VerificationCodeStore verificationCodeStore;

  private static final String TABLE_NAME = "verification_code_test";

  private static final String PHONE_NUMBER = "+14151112222";

  private static final long VALID_TIMESTAMP = Instant.now().toEpochMilli();
  private static final long EXPIRED_TIMESTAMP = Instant.now().minus(StoredVerificationCode.EXPIRATION).minus(
      Duration.ofHours(1)).toEpochMilli();

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = DynamoDbExtension.builder()
      .tableName(TABLE_NAME)
      .hashKey(VerificationCodeStore.KEY_E164)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(VerificationCodeStore.KEY_E164)
          .attributeType(ScalarAttributeType.S)
          .build())
      .build();

  @BeforeEach
  void setUp() {
    verificationCodeStore = new VerificationCodeStore(DYNAMO_DB_EXTENSION.getDynamoDbClient(), TABLE_NAME);
  }

  @Test
  void testStoreAndFind() {
    assertEquals(Optional.empty(), verificationCodeStore.findForNumber(PHONE_NUMBER));

    final StoredVerificationCode originalCode = new StoredVerificationCode("1234", VALID_TIMESTAMP, "abcd", "session".getBytes(StandardCharsets.UTF_8));
    final StoredVerificationCode secondCode = new StoredVerificationCode("5678", VALID_TIMESTAMP, "efgh", "changed-session".getBytes(StandardCharsets.UTF_8));

    verificationCodeStore.insert(PHONE_NUMBER, originalCode);
    {
      final Optional<StoredVerificationCode> maybeCode = verificationCodeStore.findForNumber(PHONE_NUMBER);

      assertTrue(maybeCode.isPresent());
      assertTrue(storedVerificationCodesAreEqual(originalCode, maybeCode.get()));
    }

    verificationCodeStore.insert(PHONE_NUMBER, secondCode);
    {
      final Optional<StoredVerificationCode> maybeCode = verificationCodeStore.findForNumber(PHONE_NUMBER);

      assertTrue(maybeCode.isPresent());
      assertTrue(storedVerificationCodesAreEqual(secondCode, maybeCode.get()));
    }
  }

  @Test
  void testRemove() {
    assertEquals(Optional.empty(), verificationCodeStore.findForNumber(PHONE_NUMBER));

    verificationCodeStore.insert(PHONE_NUMBER, new StoredVerificationCode("1234", VALID_TIMESTAMP, "abcd", "session".getBytes(StandardCharsets.UTF_8)));
    assertTrue(verificationCodeStore.findForNumber(PHONE_NUMBER).isPresent());

    verificationCodeStore.remove(PHONE_NUMBER);
    assertFalse(verificationCodeStore.findForNumber(PHONE_NUMBER).isPresent());

    verificationCodeStore.insert(PHONE_NUMBER, new StoredVerificationCode("1234", EXPIRED_TIMESTAMP, "abcd", "session".getBytes(StandardCharsets.UTF_8)));
    assertFalse(verificationCodeStore.findForNumber(PHONE_NUMBER).isPresent());
  }

  private static boolean storedVerificationCodesAreEqual(final StoredVerificationCode first, final StoredVerificationCode second) {
    if (first == null && second == null) {
      return true;
    } else if (first == null || second == null) {
      return false;
    }

    return Objects.equals(first.code(), second.code()) &&
        first.timestamp() == second.timestamp() &&
        Objects.equals(first.pushCode(), second.pushCode()) &&
        Arrays.equals(first.sessionId(), second.sessionId());
  }
}
