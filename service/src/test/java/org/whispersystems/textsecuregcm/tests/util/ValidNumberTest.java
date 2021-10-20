/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ValidNumberTest {

  @ParameterizedTest
  @MethodSource
  void isValid(final String number, final boolean expectValid) {
    assertEquals(expectValid, Util.isValidNumber(number));
  }

  private static Stream<Arguments> isValid() {
    return Stream.of(
        // Valid numbers
        Arguments.of("+14151231234", true),
        Arguments.of("+71234567890", true),
        Arguments.of("+447535742222", true),
        Arguments.of("+4915174108888", true),

        // Invalid e164s
        Arguments.of("+141512312341", false),
        Arguments.of("+712345678901", false),
        Arguments.of("+4475357422221", false),
        Arguments.of("+491517410888811111", false),

        // Non-e164s
        Arguments.of("+1 415 123 1234", false),
        Arguments.of("+1 (415) 123-1234", false),
        Arguments.of("+1 415)123-1234", false),
        Arguments.of("71234567890", false),
        Arguments.of("001447535742222", false),
        Arguments.of(" +14151231234", false),
        Arguments.of("+1415123123a", false),

        // Short region
        Arguments.of("+298123456", true),
        Arguments.of("+299123456", true),
        Arguments.of("+376123456", true),
        Arguments.of("+68512345", true),
        Arguments.of("+689123456", true)
    );
  }
}
