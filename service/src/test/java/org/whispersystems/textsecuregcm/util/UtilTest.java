/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class UtilTest {
  // libphonenumber 8.13.50 and on generate new-format numbers for Benin
  private static final String NEW_FORMAT_BENIN_E164_STRING = PhoneNumberUtil.getInstance()
      .format(PhoneNumberUtil.getInstance().getExampleNumber("BJ"), PhoneNumberUtil.PhoneNumberFormat.E164);
  private static final String OLD_FORMAT_BENIN_E164_STRING = NEW_FORMAT_BENIN_E164_STRING.replaceFirst("01", "");

  @ParameterizedTest
  @MethodSource
  void getAlternateForms(final String phoneNumber, final List<String> expectedAlternateForms) {
    assertEquals(expectedAlternateForms, Util.getAlternateForms(phoneNumber));
  }

  static List<Arguments> getAlternateForms() {
    final String usE164 = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    return List.of(
        Arguments.of(usE164, List.of(usE164)),
        Arguments.of(NEW_FORMAT_BENIN_E164_STRING, List.of(NEW_FORMAT_BENIN_E164_STRING, OLD_FORMAT_BENIN_E164_STRING)),
        Arguments.of(OLD_FORMAT_BENIN_E164_STRING, List.of(OLD_FORMAT_BENIN_E164_STRING, NEW_FORMAT_BENIN_E164_STRING))
    );
  }

  @Test
  void getCanonicalNumber() {
    final String usE164 = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);
    assertEquals(Optional.of(usE164), Util.getCanonicalNumber(List.of(usE164)));

    final String newFormatBeninE164 = PhoneNumberUtil.getInstance()
        .format(PhoneNumberUtil.getInstance().getExampleNumber("BJ"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final String oldFormatBeninE164 = newFormatBeninE164.replaceFirst("01", "");
    assertEquals(Optional.of(newFormatBeninE164), Util.getCanonicalNumber(List.of(oldFormatBeninE164, newFormatBeninE164)));

    assertEquals(Optional.empty(), Util.getCanonicalNumber(List.of()));
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

  @ParameterizedTest
  @MethodSource
  void isOldFormatBeninPhoneNumber4(final Phonenumber.PhoneNumber beninNumber, final boolean isOldFormatBeninNumber) {
    if (isOldFormatBeninNumber) {
      assertTrue(Util.isOldFormatBeninPhoneNumber(beninNumber));
    } else {
      assertFalse(Util.isOldFormatBeninPhoneNumber(beninNumber));
    }
  }

  private static Stream<Arguments> isOldFormatBeninPhoneNumber4() throws NumberParseException {
    final Phonenumber.PhoneNumber oldFormatBeninE164 = PhoneNumberUtil.getInstance().parse(OLD_FORMAT_BENIN_E164_STRING, null);
    final Phonenumber.PhoneNumber newFormatBeninE164 = PhoneNumberUtil.getInstance().parse(NEW_FORMAT_BENIN_E164_STRING, null);

    return Stream.of(
        Arguments.of(oldFormatBeninE164, true),
        Arguments.of(newFormatBeninE164, false),
        Arguments.of(PhoneNumberUtil.getInstance().getExampleNumber("US"), false)
    );
  }

  @ParameterizedTest
  @MethodSource
  void normalizeBeninPhoneNumber(final Phonenumber.PhoneNumber beninNumber, final Phonenumber.PhoneNumber expectedBeninNumber, @Nullable Class<? extends Throwable> exception)
      throws Exception {
    if (exception == null) {
      assertTrue(expectedBeninNumber.exactlySameAs(Util.canonicalizePhoneNumber(beninNumber)));
    } else {
      assertThrows(exception, () -> Util.canonicalizePhoneNumber(beninNumber));
    }
  }

  private static Stream<Arguments> normalizeBeninPhoneNumber() throws NumberParseException {
    final Phonenumber.PhoneNumber oldFormatBeninPhoneNumber = PhoneNumberUtil.getInstance().parse(OLD_FORMAT_BENIN_E164_STRING, null);
    final Phonenumber.PhoneNumber newFormatBeninPhoneNumber = PhoneNumberUtil.getInstance().parse(NEW_FORMAT_BENIN_E164_STRING, null);
    final Phonenumber.PhoneNumber usPhoneNumber = PhoneNumberUtil.getInstance().getExampleNumber("US");
    return Stream.of(
        Arguments.of(newFormatBeninPhoneNumber, newFormatBeninPhoneNumber, null),
        Arguments.of(oldFormatBeninPhoneNumber, null, ObsoletePhoneNumberFormatException.class),
        Arguments.of(usPhoneNumber, usPhoneNumber, null)
    );
  }
}
