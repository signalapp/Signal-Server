/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

/**
 * @param boost      configuration for individual donations
 * @param gift       configuration for gift donations
 * @param currencies map of lower-cased ISO 3 currency codes and the suggested donation amounts in that currency
 */
public record OneTimeDonationConfiguration(@Valid ExpiringLevelConfiguration boost,
                                           @Valid ExpiringLevelConfiguration gift,
                                           Map<String, @Valid OneTimeDonationCurrencyConfiguration> currencies,
                                           BigDecimal sepaMaximumEuros) {

  /**
   * @param badge      the numeric donation level ID
   * @param level      the badge ID associated with the level
   * @param expiration the duration after which the level expires
   */
  public record ExpiringLevelConfiguration(@NotEmpty String badge, @Positive long level, Duration expiration) {

  }
}
