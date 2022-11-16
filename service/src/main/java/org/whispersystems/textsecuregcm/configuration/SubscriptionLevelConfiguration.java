/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class SubscriptionLevelConfiguration {

  private final String badge;
  private final Map<String, SubscriptionPriceConfiguration> prices;

  @JsonCreator
  public SubscriptionLevelConfiguration(
      @JsonProperty("badge") @NotEmpty String badge,
      @JsonProperty("prices") @Valid Map<@NotEmpty String, @NotNull @Valid SubscriptionPriceConfiguration> prices) {
    this.badge = badge;
    this.prices = prices;
  }

  public String getBadge() {
    return badge;
  }

  public Map<String, SubscriptionPriceConfiguration> getPrices() {
    return prices;
  }
}
