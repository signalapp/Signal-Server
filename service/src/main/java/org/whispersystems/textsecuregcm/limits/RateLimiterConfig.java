/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import java.time.Duration;

public record RateLimiterConfig(int bucketSize, Duration permitRegenerationDuration, boolean failOpen) {

  public double leakRatePerMillis() {
    return 1.0 / (permitRegenerationDuration.toNanos() / 1e6);
  }

  @AssertTrue
  @Schema(hidden = true)
  public boolean hasPositiveRegenerationRate() {
    try {
      return permitRegenerationDuration.toNanos() > 0;
    } catch (final ArithmeticException e) {
      // The duration was too large to fit in a long, so it's definitely positive
      return true;
    }
  }
}
