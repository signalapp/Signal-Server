/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ValidNumberTest {

  @ParameterizedTest
  @ValueSource(strings = {
      "+447700900111",
      "+14151231234",
      "+71234567890",
      "+447535742222",
      "+4915174108888",
      "+2250707312345",
      "+298123456",
      "+299123456",
      "+376123456",
      "+68512345",
      "+689123456",
      "+80011111111"})
  void requireNormalizedNumber(final String number) {
    assertDoesNotThrow(() -> Util.requireNormalizedNumber(number));
  }

  @Test
  void requireNormalizedNumberNull() {
    assertThrows(ImpossiblePhoneNumberException.class, () -> Util.requireNormalizedNumber(null));
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
    assertThrows(ImpossiblePhoneNumberException.class, () -> Util.requireNormalizedNumber(number));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "+4407700900111",
      "+49493023125000", // double country code - this e164 is "possible"
      "+1 415 123 1234",
      "+1 (415) 123-1234",
      "+1 415)123-1234",
      " +14151231234"})
  void requireNormalizedNumberNonNormalized(final String number) {
    assertThrows(NonNormalizedPhoneNumberException.class, () -> Util.requireNormalizedNumber(number));
  }
}
