/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import io.dropwizard.validation.ValidationMethod;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class BadgesConfiguration {
  private final List<BadgeConfiguration> badges;
  private final List<String> badgeIdsEnabledForAll;
  private final Map<Long, String> receiptLevels;

  @JsonCreator
  public BadgesConfiguration(
      @JsonProperty("badges") @JsonSetter(nulls = Nulls.AS_EMPTY) final List<BadgeConfiguration> badges,
      @JsonProperty("badgeIdsEnabledForAll") @JsonSetter(nulls = Nulls.AS_EMPTY) final List<String> badgeIdsEnabledForAll,
      @JsonProperty("receiptLevels") @JsonSetter(nulls = Nulls.AS_EMPTY) final Map<Long, String> receiptLevels) {
    this.badges = Objects.requireNonNull(badges);
    this.badgeIdsEnabledForAll = Objects.requireNonNull(badgeIdsEnabledForAll);
    this.receiptLevels = Objects.requireNonNull(receiptLevels);
  }

  @Valid
  @NotNull
  public List<BadgeConfiguration> getBadges() {
    return badges;
  }

  @Valid
  @NotNull
  public List<String> getBadgeIdsEnabledForAll() {
    return badgeIdsEnabledForAll;
  }

  @Valid
  @NotNull
  public Map<Long, String> getReceiptLevels() {
    return receiptLevels;
  }

  @JsonIgnore
  @ValidationMethod(message = "contains receipt level mappings that are not configured badges")
  public boolean isAllReceiptLevelsConfigured() {
    final Set<String> badgeNames = badges.stream().map(BadgeConfiguration::getId).collect(Collectors.toSet());
    return badgeNames.containsAll(receiptLevels.values());
  }
}
