/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class UsernameValidatorTest {

  @ParameterizedTest
  @MethodSource
  void isValid(final String username, final boolean expectValid) {
    final UsernameValidator usernameValidator = new UsernameValidator();

    assertEquals(expectValid, usernameValidator.isValid(username, null));
  }

  private static Stream<Arguments> isValid() {
    return Stream.of(
        Arguments.of("test", true),
        Arguments.of("_test", true),
        Arguments.of("test123", true),
        Arguments.of("a", false), // Too short
        Arguments.of("thisIsAReallyReallyReallyLongUsernameThatWeWouldNotAllow", false),
        Arguments.of("Illegal character", false),
        Arguments.of("0test", false), // Illegal first character
        Arguments.of("pаypal", false), // Unicode confusable characters
        Arguments.of("test\uD83D\uDC4E", false), // Emoji
        Arguments.of(" ", false),
        Arguments.of("", false),
        Arguments.of(null, false)
    );
  }

  @ParameterizedTest
  @MethodSource
  void getCanonicalUsername(final String username, final String expectedCanonicalUsername) {
    assertEquals(expectedCanonicalUsername, UsernameValidator.getCanonicalUsername(username));
  }

  private static Stream<Arguments> getCanonicalUsername() {
    return Stream.of(
        Arguments.of("test", "test"),
        Arguments.of("TEst", "test"),
        Arguments.of("t_e_S_T", "t_e_s_t"),
        Arguments.of(null, null)
    );
  }
}
