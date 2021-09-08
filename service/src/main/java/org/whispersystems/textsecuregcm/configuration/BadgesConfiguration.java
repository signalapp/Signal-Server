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

  @JsonCreator
  public BadgesConfiguration(
      @JsonProperty("badges") @JsonSetter(nulls = Nulls.AS_EMPTY) final List<BadgeConfiguration> badges) {
    this.badges = Objects.requireNonNull(badges);
  }

  @Valid
  @NotNull
  public List<BadgeConfiguration> getBadges() {
    return badges;
  }
}
