/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class VerificationCodeStoreTest {

  private static final String PHONE_NUMBER = "+14151112222";

  protected abstract VerificationCodeStore getVerificationCodeStore();

  protected abstract boolean expectNullPushCode();

  protected abstract boolean expectEmptyTwilioSid();

  @Test
  void testStoreAndFind() {
    assertEquals(Optional.empty(), getVerificationCodeStore().findForNumber(PHONE_NUMBER));

    final StoredVerificationCode originalCode = new StoredVerificationCode("1234", 1111, "abcd", "0987");
    final StoredVerificationCode secondCode = new StoredVerificationCode("5678", 2222, "efgh", "7890");

    getVerificationCodeStore().insert(PHONE_NUMBER, originalCode);

    {
      final Optional<StoredVerificationCode> maybeRetrievedCode = getVerificationCodeStore().findForNumber(PHONE_NUMBER);

      assertTrue(maybeRetrievedCode.isPresent());
      compareStoredVerificationCode(originalCode, maybeRetrievedCode.get());
    }

    getVerificationCodeStore().insert(PHONE_NUMBER, secondCode);

    {
      final Optional<StoredVerificationCode> maybeRetrievedCode = getVerificationCodeStore().findForNumber(PHONE_NUMBER);

      assertTrue(maybeRetrievedCode.isPresent());
      compareStoredVerificationCode(secondCode, maybeRetrievedCode.get());
    }
  }

  @Test
  void testRemove() {
    assertEquals(Optional.empty(), getVerificationCodeStore().findForNumber(PHONE_NUMBER));

    getVerificationCodeStore().insert(PHONE_NUMBER, new StoredVerificationCode("1234", 1111, "abcd", "0987"));
    assertTrue(getVerificationCodeStore().findForNumber(PHONE_NUMBER).isPresent());

    getVerificationCodeStore().remove(PHONE_NUMBER);
    assertFalse(getVerificationCodeStore().findForNumber(PHONE_NUMBER).isPresent());
  }

  private void compareStoredVerificationCode(final StoredVerificationCode original, final StoredVerificationCode retrieved) {
    assertEquals(original.getCode(), retrieved.getCode());
    assertEquals(original.getTimestamp(), retrieved.getTimestamp());

    if (expectNullPushCode()) {
      assertNull(retrieved.getPushCode());
    } else {
      assertEquals(original.getPushCode(), retrieved.getPushCode());
    }

    if (expectEmptyTwilioSid()) {
      assertEquals(Optional.empty(), retrieved.getTwilioVerificationSid());
    } else {
      assertEquals(original.getTwilioVerificationSid(), retrieved.getTwilioVerificationSid());
    }
  }
}
