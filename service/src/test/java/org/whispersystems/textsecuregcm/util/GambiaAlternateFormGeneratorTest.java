/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.i18n.phonenumbers.NumberParseException;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class GambiaAlternateFormGeneratorTest {

  @ParameterizedTest
  @MethodSource
  void getAlternateForms(final String e164, final List<String> alternateForms) throws NumberParseException {
    assertEquals(alternateForms, new GambiaAlternateFormGenerator().getAlternateForms(e164));
  }

  private static List<Arguments> getAlternateForms() {
    return List.of(
        Arguments.argumentSet("Legacy Qcell number", "+2203123456", List.of("+2203123456", "+220833123456")),
        Arguments.argumentSet("Legacy Comium number", "+2206123456", List.of("+2206123456", "+220866123456")),
        Arguments.argumentSet("Legacy Africell number", "+2202123456", List.of("+2202123456", "+220872123456")),
        Arguments.argumentSet("Legacy Gamcel number", "+2209123456", List.of("+2209123456")),
        Arguments.argumentSet("New-style Qcell number", "+220833123456", List.of("+220833123456", "+2203123456")),
        Arguments.argumentSet("New-style Comium number", "+220866123456", List.of("+220866123456", "+2206123456")),
        Arguments.argumentSet("New-style Africell number", "+220872123456", List.of("+220872123456", "+2202123456"))
    );
  }

  @ParameterizedTest
  @CsvSource({
      "3123456,QCELL",
      "5012345,QCELL",
      "5612345,",
      "835700011,QCELL",
      "2123456,AFRICELL",
      "7123456,AFRICELL",
      "4012345,AFRICELL",
      "4212345,",
      "872123456,AFRICELL",
      "6123456,COMIUM",
      "8712345,COMIUM",
      "8812345,",
      "868712345,COMIUM",
      "881234567,"
  })
  void getCarrier(final String nationalSignificantNumber,
      @Nullable final GambiaAlternateFormGenerator.Carrier expectedCarrier) {

    assertEquals(Optional.ofNullable(expectedCarrier),
        GambiaAlternateFormGenerator.getCarrier(nationalSignificantNumber));
  }

}
