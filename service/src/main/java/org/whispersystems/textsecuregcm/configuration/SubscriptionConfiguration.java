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
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.backup.BackupTier;

public class SubscriptionConfiguration {

  private final Duration badgeGracePeriod;
  private final Duration badgeExpiration;

  private final Duration backupExpiration;
  private final Map<Long, SubscriptionLevelConfiguration.Donation> donationLevels;
  private final Map<Long, SubscriptionLevelConfiguration.Backup> backupLevels;

  @JsonCreator
  public SubscriptionConfiguration(
      @JsonProperty("badgeGracePeriod") @Valid Duration badgeGracePeriod,
      @JsonProperty("badgeExpiration") @Valid Duration badgeExpiration,
      @JsonProperty("backupExpiration") @Valid Duration backupExpiration,
      @JsonProperty("levels") @Valid Map<@NotNull @Min(1) Long, SubscriptionLevelConfiguration.@NotNull @Valid Donation> donationLevels,
      @JsonProperty("backupLevels") @Valid Map<@NotNull @Min(1) Long, SubscriptionLevelConfiguration.@NotNull @Valid Backup> backupLevels) {
    this.badgeGracePeriod = badgeGracePeriod;
    this.badgeExpiration = badgeExpiration;
    this.donationLevels = donationLevels;
    this.backupExpiration = backupExpiration;
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
        .allMatch(level -> BackupTier.fromReceiptLevel(level).orElse(BackupTier.NONE) != BackupTier.NONE);

    // None of the donation levels correspond to backup levels
    final boolean donationLevelsDontMatch = donationLevels.keySet().stream()
        .allMatch(level -> BackupTier.fromReceiptLevel(level).orElse(BackupTier.NONE) == BackupTier.NONE);

    // The configured donation and backup levels don't intersect
    final boolean levelsDontIntersect = Sets.intersection(backupLevels.keySet(), donationLevels.keySet()).isEmpty();

    return backupLevelsMatch && donationLevelsDontMatch && levelsDontIntersect;
  }

  @JsonIgnore
  @ValidationMethod(message = "has a mismatch between the levels supported currencies")
  public boolean isCurrencyListSameAcrossAllLevels() {
    final Map<Long, SubscriptionLevelConfiguration> subscriptionLevels = Stream
        .concat(donationLevels.entrySet().stream(), backupLevels.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Optional<SubscriptionLevelConfiguration> any = subscriptionLevels.values().stream().findAny();
    if (any.isEmpty()) {
      return true;
    }

    Set<String> currencies = any.get().prices().keySet();
    return subscriptionLevels.values().stream().allMatch(level -> currencies.equals(level.prices().keySet()));
  }
}
