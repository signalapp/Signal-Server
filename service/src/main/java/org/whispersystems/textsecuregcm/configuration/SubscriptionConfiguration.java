/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import io.dropwizard.validation.ValidationMethod;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.whispersystems.textsecuregcm.backup.BackupLevelUtil;

public class SubscriptionConfiguration {

  private final Duration badgeGracePeriod;
  private final Duration badgeExpiration;

  private final Duration backupExpiration;
  private final Duration backupGracePeriod;
  private final Duration backupFreeTierMediaDuration;
  private final Map<Long, SubscriptionLevelConfiguration.Donation> donationLevels;
  private final Map<Long, SubscriptionLevelConfiguration.Backup> backupLevels;

  @JsonCreator
  public SubscriptionConfiguration(
      @JsonProperty("badgeGracePeriod") @Valid Duration badgeGracePeriod,
      @JsonProperty("badgeExpiration") @Valid Duration badgeExpiration,
      @JsonProperty("backupExpiration") @Valid Duration backupExpiration,
      @JsonProperty("backupGracePeriod") @Valid Duration backupGracePeriod,
      @JsonProperty("backupFreeTierMediaDuration") @Valid Duration backupFreeTierMediaDuration,
      @JsonProperty("levels") @Valid Map<@NotNull @Min(1) Long, SubscriptionLevelConfiguration.@NotNull @Valid Donation> donationLevels,
      @JsonProperty("backupLevels") @Valid Map<@NotNull @Min(1) Long, SubscriptionLevelConfiguration.@NotNull @Valid Backup> backupLevels) {
    this.badgeGracePeriod = badgeGracePeriod;
    this.badgeExpiration = badgeExpiration;
    this.backupFreeTierMediaDuration = backupFreeTierMediaDuration;
    this.donationLevels = donationLevels;
    this.backupExpiration = backupExpiration;
    this.backupGracePeriod = backupGracePeriod;
    this.backupLevels = backupLevels == null ? Collections.emptyMap() : backupLevels;
  }

  public Duration getBadgeGracePeriod() {
    return badgeGracePeriod;
  }

  // This is the badge expiration time starting from when a payment successfully completes
  public Duration getBadgeExpiration() {
    return badgeExpiration;
  }

  public Duration getBackupExpiration() {
    return backupExpiration;
  }

  public Duration getBackupGracePeriod() {
    return backupGracePeriod;
  }

  public SubscriptionLevelConfiguration getSubscriptionLevel(long level) {
    return Optional
        .<SubscriptionLevelConfiguration>ofNullable(this.donationLevels.get(level))
        .orElse(this.backupLevels.get(level));
  }

  public Map<Long, SubscriptionLevelConfiguration.Donation> getDonationLevels() {
    return donationLevels;
  }

  public Map<Long, SubscriptionLevelConfiguration.Backup> getBackupLevels() {
    return backupLevels;
  }

  @JsonIgnore
  @ValidationMethod(message = "Backup levels and donation levels should not intersect")
  public boolean areLevelConstraintsSatisfied() {
    // We have a tier for all configured backup levels
    final boolean backupLevelsMatch = backupLevels.keySet()
        .stream()
        .allMatch(SubscriptionConfiguration::isValidBackupLevel);

    // None of the donation levels correspond to backup levels
    final boolean donationLevelsDontMatch = donationLevels.keySet().stream()
        .allMatch(Predicate.not(SubscriptionConfiguration::isValidBackupLevel));

    // The configured donation and backup levels don't intersect
    final boolean levelsDontIntersect = Sets.intersection(backupLevels.keySet(), donationLevels.keySet()).isEmpty();

    return backupLevelsMatch && donationLevelsDontMatch && levelsDontIntersect;
  }

  @JsonIgnore
  @ValidationMethod(message = "has a mismatch between the levels supported currencies")
  public boolean isCurrencyListSameAcrossAllLevels() {
    return isCurrencyListSameAccrossLevelConfigurations(donationLevels)
        && isCurrencyListSameAccrossLevelConfigurations(backupLevels);
  }

  private static boolean isCurrencyListSameAccrossLevelConfigurations(
      Map<Long, ? extends SubscriptionLevelConfiguration> subscriptionLevels) {
    Optional<? extends SubscriptionLevelConfiguration> any = subscriptionLevels.values().stream().findAny();
    if (any.isEmpty()) {
      return true;
    }

    Set<String> currencies = any.get().prices().keySet();
    return subscriptionLevels.values().stream().allMatch(level -> currencies.equals(level.prices().keySet()));
  }

  public Duration getbackupFreeTierMediaDuration() {
    return backupFreeTierMediaDuration;
  }

  private static boolean isValidBackupLevel(final long receiptLevel) {
    try {
      BackupLevelUtil.fromReceiptLevel(receiptLevel);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

}
