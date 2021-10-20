/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.util.ImpossibleNumberException;
import org.whispersystems.textsecuregcm.util.NonNormalizedNumberException;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

  @ParameterizedTest
  @ValueSource(strings = {
      "+447700900111",
      "+14151231234",
      "+71234567890",
      "+447535742222",
      "+4915174108888",
      "+298123456",
      "+299123456",
      "+376123456",
      "+68512345",
      "+689123456"})
  void requireNormalizedNumber(final String number) {
    assertDoesNotThrow(() -> Util.requireNormalizedNumber(number));
  }

  @Test
  void requireNormalizedNumberNull() {
    assertThrows(ImpossibleNumberException.class, () -> Util.requireNormalizedNumber(null));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "Definitely not a phone number at all",
      "+141512312341",
      "+712345678901",
      "+4475357422221",
      "+491517410888811111",
      "71234567890",
      "001447535742222",
      "+1415123123a"
  })
  void requireNormalizedNumberImpossibleNumber(final String number) {
    assertThrows(ImpossibleNumberException.class, () -> Util.requireNormalizedNumber(number));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "+4407700900111",
      "+1 415 123 1234",
      "+1 (415) 123-1234",
      "+1 415)123-1234",
      " +14151231234"})
  void requireNormalizedNumberNonNormalized(final String number) {
    assertThrows(NonNormalizedNumberException.class, () -> Util.requireNormalizedNumber(number));
  }
}
