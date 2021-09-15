/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.badges;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ResourceBundle;
import java.util.ResourceBundle.Control;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.storage.AccountBadge;

public class ConfiguredProfileBadgeConverter implements ProfileBadgeConverter {

  private static final int MAX_LOCALES = 15;
  @VisibleForTesting
  static final String BASE_NAME = "org.signal.badges.Badges";

  private final Clock clock;
  private final Map<String, BadgeConfiguration> knownBadges;
  private final ResourceBundleFactory resourceBundleFactory;

  public ConfiguredProfileBadgeConverter(
      final Clock clock,
      final BadgesConfiguration badgesConfiguration) {
    this(clock, badgesConfiguration, ResourceBundle::getBundle);
  }

  @VisibleForTesting
  public ConfiguredProfileBadgeConverter(
      final Clock clock,
      final BadgesConfiguration badgesConfiguration,
      final ResourceBundleFactory resourceBundleFactory) {
    this.clock = clock;
    this.knownBadges = badgesConfiguration.getBadges().stream()
        .collect(Collectors.toMap(BadgeConfiguration::getName, Function.identity()));
    this.resourceBundleFactory = resourceBundleFactory;
  }

  @Override
  public List<Badge> convert(
      final List<Locale> acceptableLanguages,
      final List<AccountBadge> accountBadges) {
    if (accountBadges.isEmpty()) {
      return List.of();
    }

    final Instant now = clock.instant();

    final List<Locale> acceptableLocales = acceptableLanguages.stream().limit(MAX_LOCALES).distinct()
        .collect(Collectors.toList());
    final Locale desiredLocale = acceptableLocales.isEmpty() ? Locale.getDefault() : acceptableLocales.get(0);

    // define a control with a fallback order as specified in the header
    ResourceBundle.Control control = new Control() {
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
        final int localeIndex = acceptableLocales.indexOf(locale);
        if (localeIndex < 0 || localeIndex >= acceptableLocales.size() - 1) {
          return Locale.getDefault();
        }
        // [0, acceptableLocales.size() - 2] is now the possible range for localeIndex
        return acceptableLocales.get(localeIndex + 1);
      }
    };

    final ResourceBundle resourceBundle = resourceBundleFactory.createBundle(BASE_NAME, desiredLocale, control);
    return accountBadges.stream()
        .filter(accountBadge -> accountBadge.isVisible()
            && now.isBefore(accountBadge.getExpiration())
            && knownBadges.containsKey(accountBadge.getName()))
        .map(accountBadge -> new Badge(knownBadges.get(accountBadge.getName()).getImageUrl(),
            resourceBundle.getString(accountBadge.getName() + "_name"),
            resourceBundle.getString(accountBadge.getName() + "_description")))
        .collect(Collectors.toList());
  }
}
