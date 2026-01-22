/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import jakarta.validation.constraints.NotNull;
import java.time.Duration;

public record DynamicCarrierDataLookupConfiguration(boolean enabled, @NotNull Duration maxCacheAge) {

  public static Duration DEFAULT_MAX_CACHE_AGE = Duration.ofDays(7);

  public DynamicCarrierDataLookupConfiguration() {
    this(false, DEFAULT_MAX_CACHE_AGE);
  }

  public DynamicCarrierDataLookupConfiguration {
    if (maxCacheAge == null) {
      maxCacheAge = DEFAULT_MAX_CACHE_AGE;
    }
  }
}
