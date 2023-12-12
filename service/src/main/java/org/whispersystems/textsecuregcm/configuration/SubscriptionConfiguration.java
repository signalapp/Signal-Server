/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.validation.ValidationMethod;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class SubscriptionConfiguration {

  private final Duration badgeGracePeriod;
  private final Duration badgeExpiration;
  private final Map<Long, SubscriptionLevelConfiguration> levels;

  @JsonCreator
  public SubscriptionConfiguration(
      @JsonProperty("badgeGracePeriod") @Valid Duration badgeGracePeriod,
      @JsonProperty("badgeExpiration") @Valid Duration badgeExpiration,
      @JsonProperty("levels") @Valid Map<@NotNull @Min(1) Long, @NotNull @Valid SubscriptionLevelConfiguration> levels) {
    this.badgeGracePeriod = badgeGracePeriod;
    this.badgeExpiration = badgeExpiration;
    this.levels = levels;
  }

  public Duration getBadgeGracePeriod() {
    return badgeGracePeriod;
  }

  // This is the badge expiration time starting from when a payment successfully completes
  public Duration getBadgeExpiration() {
    return badgeExpiration;
  }

  public Map<Long, SubscriptionLevelConfiguration> getLevels() {
    return levels;
  }

  @JsonIgnore
  @ValidationMethod(message = "has a mismatch between the levels supported currencies")
  public boolean isCurrencyListSameAcrossAllLevels() {
    Optional<SubscriptionLevelConfiguration> any = levels.values().stream().findAny();
    if (any.isEmpty()) {
      return true;
    }

    Set<String> currencies = any.get().getPrices().keySet();
    return levels.values().stream().allMatch(level -> currencies.equals(level.getPrices().keySet()));
  }
}
