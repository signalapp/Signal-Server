/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.ResourceBundle;
import javax.annotation.Nonnull;
import org.signal.i18n.HeaderControlledResourceBundleLookup;

public class PayPalDonationsTranslator {

  public static final String ONE_TIME_DONATION_LINE_ITEM_KEY = "oneTime.donationLineItemName";

  private static final String BASE_NAME = "org.signal.donations.PayPal";

  private final HeaderControlledResourceBundleLookup headerControlledResourceBundleLookup;

  public PayPalDonationsTranslator(
      @Nonnull final HeaderControlledResourceBundleLookup headerControlledResourceBundleLookup) {
    this.headerControlledResourceBundleLookup = Objects.requireNonNull(headerControlledResourceBundleLookup);
  }

  public String translate(final List<Locale> acceptableLanguages, final String key) {
    final ResourceBundle resourceBundle = headerControlledResourceBundleLookup.getResourceBundle(BASE_NAME,
        acceptableLanguages);
    return resourceBundle.getString(key);
  }
}
