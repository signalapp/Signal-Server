/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.i18n;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.ResourceBundle;
import java.util.ResourceBundle.Control;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class HeaderControlledResourceBundleLookup {

  private static final int MAX_LOCALES = 15;

  private final ResourceBundleFactory resourceBundleFactory;

  public HeaderControlledResourceBundleLookup() {
    this(ResourceBundle::getBundle);
  }

  @VisibleForTesting
  public HeaderControlledResourceBundleLookup(
      @Nonnull final ResourceBundleFactory resourceBundleFactory) {
    this.resourceBundleFactory = Objects.requireNonNull(resourceBundleFactory);
  }

  @Nonnull
  private List<Locale> getAcceptableLocales(final List<Locale> acceptableLanguages) {
    return acceptableLanguages.stream().limit(MAX_LOCALES).distinct().collect(Collectors.toList());
  }

  @Nonnull
  public ResourceBundle getResourceBundle(final String baseName, final List<Locale> acceptableLocales) {
    final List<Locale> deduplicatedLocales = getAcceptableLocales(acceptableLocales);
    final Locale desiredLocale = deduplicatedLocales.isEmpty() ? Locale.getDefault() : deduplicatedLocales.get(0);
    // define a control with a fallback order as specified in the header
    Control control = new Control() {
      @Override
      public List<String> getFormats(final String baseName) {
        Objects.requireNonNull(baseName);
        return Control.FORMAT_PROPERTIES;
      }

      @Override
      public Locale getFallbackLocale(final String baseName, final Locale locale) {
        Objects.requireNonNull(baseName);
        if (locale.equals(Locale.getDefault())) {
          return null;
        }
        final int localeIndex = deduplicatedLocales.indexOf(locale);
        if (localeIndex < 0 || localeIndex >= deduplicatedLocales.size() - 1) {
          return Locale.getDefault();
        }
        // [0, deduplicatedLocales.size() - 2] is now the possible range for localeIndex
        return deduplicatedLocales.get(localeIndex + 1);
      }
    };

    return resourceBundleFactory.createBundle(baseName, desiredLocale, control);
  }
}
