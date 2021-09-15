/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import java.util.List;
import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class BadgesConfiguration {
  private final List<BadgeConfiguration> badges;
  private final List<String> badgeIdsEnabledForAll;

  @JsonCreator
  public BadgesConfiguration(
      @JsonProperty("badges") @JsonSetter(nulls = Nulls.AS_EMPTY) final List<BadgeConfiguration> badges,
      @JsonProperty("badgeIdsEnabledForAll") @JsonSetter(nulls = Nulls.AS_EMPTY) final List<String> badgeIdsEnabledForAll) {
    this.badges = Objects.requireNonNull(badges);
    this.badgeIdsEnabledForAll = Objects.requireNonNull(badgeIdsEnabledForAll);
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
}
