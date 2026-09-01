/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import org.apache.commons.lang3.Strings;
import java.util.List;

// Benin changed phone number formats from +229 XXXXXXXX to +229 01XXXXXXXX on November 30, 2024
class BeninAlternateFormGenerator implements AlternatePhoneNumberFormGenerator {

  @Override
  public List<String> getAlternateForms(final String number) throws NumberParseException {
    final Phonenumber.PhoneNumber phoneNumber = PhoneNumberUtil.getInstance().parse(number, null);

    final String nationalSignificantNumber = PhoneNumberUtil.getInstance().getNationalSignificantNumber(phoneNumber);
    final String alternateE164;

    if (nationalSignificantNumber.length() == 10) {
      // This is a new-format number; we can get the old-format version by stripping the leading "01" from the
      // national number
      alternateE164 = "+229" + Strings.CS.removeStart(nationalSignificantNumber, "01");
    } else {
      // This is an old-format number; we can get the new-format version by adding a "01" prefix to the national
      // number
      alternateE164 = "+22901" + nationalSignificantNumber;
    }

    return number.equals(alternateE164) ? List.of(number) : List.of(number, alternateE164);
  }
}
