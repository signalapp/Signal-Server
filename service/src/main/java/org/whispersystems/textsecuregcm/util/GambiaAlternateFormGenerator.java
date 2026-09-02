/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/// Generates alternate forms of Gambian phone numbers as [specified by
/// PURA](https://pura.gm/public-notice-migration-of-the-national-mobile-numbering-plan-from-7-digit-to-9-digit-format/).
class GambiaAlternateFormGenerator implements AlternatePhoneNumberFormGenerator {

  // Please see https://pura.gm/wp-content/uploads/2026/07/PURA_Website_NNP_Migration_Public_Notice.pdf for number
  // format details.
  enum Carrier {
    QCELL("83", Pattern.compile("^(3\\d{6})|(5[012345789]\\d{5})$")),
    COMIUM("86", Pattern.compile("^(6\\d{6})|(8[4567]\\d{5})$")),
    AFRICELL("87", Pattern.compile("^([27]\\d{6})|(4[015]\\d{5})$"));

    private final String newPrefix;
    private final Pattern legacyNumberPattern;

    Carrier(final String newPrefix, final Pattern legacyNumberPattern) {
      this.newPrefix = newPrefix;
      this.legacyNumberPattern = legacyNumberPattern;
    }
  }

  @Override
  public List<String> getAlternateForms(final String number) throws NumberParseException {
    final Phonenumber.PhoneNumber phoneNumber = PhoneNumberUtil.getInstance().parse(number, null);
    final String nationalSignificantNumber = PhoneNumberUtil.getInstance().getNationalSignificantNumber(phoneNumber);

    final Optional<String> maybeAlternateForm = getCarrier(nationalSignificantNumber)
        .map(carrier -> {
              if (nationalSignificantNumber.length() == 7) {
                // This is a legacy number, and we need to add the carrier's prefix to get the updated form
                return "+220" + carrier.newPrefix + nationalSignificantNumber;
              } else {
                // This is a new-style number, and we need to strip the carrier's prefix to get the legacy form
                return "+220" + nationalSignificantNumber.substring(carrier.newPrefix.length());
              }
            });

    return maybeAlternateForm
        .map(alternateForm -> List.of(number, alternateForm))
        .orElseGet(() -> List.of(number));
  }

  @VisibleForTesting
  static Optional<Carrier> getCarrier(final String nationalSignificantNumber) {
    if (nationalSignificantNumber.length() == 9) {
      // This might be a new-style number; see if we can find a carrier by prefix
      return Arrays.stream(Carrier.values())
          .filter(carrier -> nationalSignificantNumber.startsWith(carrier.newPrefix))
          .findFirst();
    } else if (nationalSignificantNumber.length() == 7) {
      // This might be an old-style number belonging to a carrier with a new prefix; match by pattern if we can
      return Arrays.stream(Carrier.values())
          .filter(carrier -> carrier.legacyNumberPattern.matcher(nationalSignificantNumber).matches())
          .findFirst();
    }

    return Optional.empty();
  }
}
