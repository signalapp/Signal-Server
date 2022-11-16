/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StoredVerificationCodeTest {

  @ParameterizedTest
  @MethodSource
  void isValid(final StoredVerificationCode storedVerificationCode, final String code, final boolean expectValid) {
    assertEquals(expectValid, storedVerificationCode.isValid(code));
  }

  private static Stream<Arguments> isValid() {
    return Stream.of(
        Arguments.of(
            new StoredVerificationCode("code", System.currentTimeMillis(), null, null), "code", true),
        Arguments.of(new StoredVerificationCode("code", System.currentTimeMillis(), null, null), "incorrect", false),
        Arguments.of(new StoredVerificationCode("", System.currentTimeMillis(), null, null), "", false)
    );
  }
}
