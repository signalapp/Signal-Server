/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Locale;
import java.util.MissingResourceException;
import org.junit.jupiter.api.Test;
import org.signal.i18n.HeaderControlledResourceBundleLookup;

class PayPalDonationsTranslatorTest {

  private final PayPalDonationsTranslator translator = new PayPalDonationsTranslator(
      new HeaderControlledResourceBundleLookup());

  @Test
  void testTranslate() {
    assertEquals("Donation to Signal Technology Foundation",
        translator.translate(List.of(Locale.ROOT), PayPalDonationsTranslator.ONE_TIME_DONATION_LINE_ITEM_KEY));
  }

  @Test
  void testTranslateUnknownKey() {
    assertThrows(MissingResourceException.class, () -> translator.translate(List.of(Locale.ROOT), "unknown-key"));
  }

}
