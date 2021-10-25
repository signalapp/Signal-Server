/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.badges;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ResourceBundle;
import java.util.ResourceBundle.Control;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.BadgeSvg;
import org.whispersystems.textsecuregcm.entities.SelfBadge;
import org.whispersystems.textsecuregcm.storage.AccountBadge;

public class ConfiguredProfileBadgeConverter implements ProfileBadgeConverter, BadgeTranslator {

  private static final int MAX_LOCALES = 15;
  @VisibleForTesting
  static final String BASE_NAME = "org.signal.badges.Badges";

  private final Clock clock;
  private final Map<String, BadgeConfiguration> knownBadges;
  private final List<String> badgeIdsEnabledForAll;
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
        .collect(Collectors.toMap(BadgeConfiguration::getId, Function.identity()));
    this.badgeIdsEnabledForAll = badgesConfiguration.getBadgeIdsEnabledForAll();
    this.resourceBundleFactory = resourceBundleFactory;
  }

  @Override
  public Badge translate(final List<Locale> acceptableLanguages, final String badgeId) {
    final List<Locale> acceptableLocales = getAcceptableLocales(acceptableLanguages);
    final ResourceBundle resourceBundle = getResourceBundle(acceptableLocales);
    final BadgeConfiguration configuration = knownBadges.get(badgeId);
    return newBadge(
        false,
        configuration.getId(),
        configuration.getCategory(),
        resourceBundle.getString(configuration.getId() + "_name"),
        resourceBundle.getString(configuration.getId() + "_description"),
        configuration.getSprites(),
        configuration.getSvg(),
        configuration.getSvgs(),
        null,
        false);
  }

  @Override
  public List<Badge> convert(
      final List<Locale> acceptableLanguages,
      final List<AccountBadge> accountBadges,
      final boolean isSelf) {
    if (accountBadges.isEmpty() && badgeIdsEnabledForAll.isEmpty()) {
      return List.of();
    }

    final Instant now = clock.instant();
    final List<Locale> acceptableLocales = getAcceptableLocales(acceptableLanguages);
    final ResourceBundle resourceBundle = getResourceBundle(acceptableLocales);
    List<Badge> badges = accountBadges.stream()
        .filter(accountBadge -> (isSelf || accountBadge.isVisible())
            && now.isBefore(accountBadge.getExpiration())
            && knownBadges.containsKey(accountBadge.getId()))
        .map(accountBadge -> {
          BadgeConfiguration configuration = knownBadges.get(accountBadge.getId());
          return newBadge(
              isSelf,
              accountBadge.getId(),
              configuration.getCategory(),
              resourceBundle.getString(accountBadge.getId() + "_name"),
              resourceBundle.getString(accountBadge.getId() + "_description"),
              configuration.getSprites(),
              configuration.getSvg(),
              configuration.getSvgs(),
              accountBadge.getExpiration(),
              accountBadge.isVisible());
        })
        .collect(Collectors.toCollection(ArrayList::new));
    badges.addAll(badgeIdsEnabledForAll.stream().filter(knownBadges::containsKey).map(id -> {
      BadgeConfiguration configuration = knownBadges.get(id);
      return newBadge(
          isSelf,
          id,
          configuration.getCategory(),
          resourceBundle.getString(id + "_name"),
          resourceBundle.getString(id + "_description"),
          configuration.getSprites(),
          configuration.getSvg(),
          configuration.getSvgs(),
          now.plus(Duration.ofDays(1)),
          true);
    }).collect(Collectors.toList()));
    return badges;
  }

  @Nonnull
  private ResourceBundle getResourceBundle(final List<Locale> acceptableLocales) {
    final Locale desiredLocale = acceptableLocales.isEmpty() ? Locale.getDefault() : acceptableLocales.get(0);
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
        final int localeIndex = acceptableLocales.indexOf(locale);
        if (localeIndex < 0 || localeIndex >= acceptableLocales.size() - 1) {
          return Locale.getDefault();
        }
        // [0, acceptableLocales.size() - 2] is now the possible range for localeIndex
        return acceptableLocales.get(localeIndex + 1);
      }
    };

    return resourceBundleFactory.createBundle(BASE_NAME, desiredLocale, control);
  }

  @Nonnull
  private List<Locale> getAcceptableLocales(final List<Locale> acceptableLanguages) {
    return acceptableLanguages.stream().limit(MAX_LOCALES).distinct().collect(Collectors.toList());
  }

  private Badge newBadge(
      final boolean isSelf,
      final String id,
      final String category,
      final String name,
      final String description,
      final List<String> sprites,
      final String svg,
      final List<BadgeSvg> svgs,
      final Instant expiration,
      final boolean visible) {
    if (isSelf) {
      return new SelfBadge(id, category, name, description, sprites, svg, svgs, expiration, visible);
    } else {
      return new Badge(id, category, name, description, sprites, svg, svgs);
    }
  }
}
