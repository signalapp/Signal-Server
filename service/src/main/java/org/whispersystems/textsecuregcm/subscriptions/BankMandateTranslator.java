/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.ResourceBundle;
import javax.annotation.Nonnull;
import org.signal.i18n.HeaderControlledResourceBundleLookup;

public class BankMandateTranslator {
  private static final String BASE_NAME = "org.signal.bankmandate.BankMandate";
  private final HeaderControlledResourceBundleLookup headerControlledResourceBundleLookup;

  public BankMandateTranslator(
      @Nonnull final HeaderControlledResourceBundleLookup headerControlledResourceBundleLookup) {
    this.headerControlledResourceBundleLookup = Objects.requireNonNull(headerControlledResourceBundleLookup);
  }

  public String translate(final List<Locale> acceptableLanguages, final BankTransferType bankTransferType) {
    final ResourceBundle resourceBundle = headerControlledResourceBundleLookup.getResourceBundle(BASE_NAME,
        acceptableLanguages);
    return resourceBundle.getString(getKey(bankTransferType));
  }

  private static String getKey(final BankTransferType bankTransferType) {
    return switch (bankTransferType) {
      case SEPA_DEBIT -> "SEPA_MANDATE";
    };
  }
}
