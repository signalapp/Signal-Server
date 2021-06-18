/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class StoredVerificationCodeTest {

  @ParameterizedTest
  @MethodSource
  void isValid(final StoredVerificationCode storedVerificationCode, final String code, final Instant currentTime, final boolean expectValid) {
    assertEquals(expectValid, storedVerificationCode.isValid(code, currentTime));
  }

  private static Stream<Arguments> isValid() {
    final Instant now = Instant.now();

    return Stream.of(
        Arguments.of(new StoredVerificationCode("code", now.toEpochMilli(), null, null), "code", now, true),
        Arguments.of(new StoredVerificationCode("code", now.toEpochMilli(), null, null), "incorrect", now, false),
        Arguments.of(new StoredVerificationCode("code", now.toEpochMilli(), null, null), "code", now.plus(Duration.ofHours(1)), false),
        Arguments.of(new StoredVerificationCode("", now.toEpochMilli(), null, null), "", now, false)
    );
  }
}
