/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class UtilTest {

  @ParameterizedTest
  @MethodSource
  void getAlternateForms(final String phoneNumber, final List<String> expectedAlternateForms) {
    assertEquals(expectedAlternateForms, Util.getAlternateForms(phoneNumber));
  }

  static List<Arguments> getAlternateForms() {
    final String usE164 = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    // libphonenumber 8.13.50 and on generate new-format numbers for Benin
    final String newFormatBeninE164 = PhoneNumberUtil.getInstance()
        .format(PhoneNumberUtil.getInstance().getExampleNumber("BJ"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final String oldFormatBeninE164 = newFormatBeninE164.replaceFirst("01", "");

    return List.of(
        Arguments.of(usE164, List.of(usE164)),
        Arguments.of(newFormatBeninE164, List.of(newFormatBeninE164, oldFormatBeninE164)),
        Arguments.of(oldFormatBeninE164, List.of(oldFormatBeninE164, newFormatBeninE164))
    );
  }

  @ParameterizedTest
  @CsvSource({
      "0, 1, false",
      "123456789, 1, true",
      "123456789, 123, true",
      "123456789, 456, false",
  })
  void startsWithDecimal(final long number, final long prefix, final boolean expectStartsWithPrefix) {
    assertEquals(expectStartsWithPrefix, Util.startsWithDecimal(number, prefix));
  }
}
