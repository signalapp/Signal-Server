/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import javax.validation.constraints.AssertTrue;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalDouble;

public record RateLimiterConfig(int bucketSize, OptionalDouble leakRatePerMinute, Optional<Duration> permitRegenerationDuration) {

  public double leakRatePerMillis() {
    if (leakRatePerMinute.isPresent()) {
      return leakRatePerMinute.getAsDouble() / (60.0 * 1000.0);
    } else {
      return permitRegenerationDuration.map(duration -> 1.0 / duration.toMillis())
          .orElseThrow(() -> new AssertionError("Configuration must have leak rate per minute or permit regeneration duration"));
    }
  }

  @AssertTrue
  public boolean hasExactlyOneRegenerationRate() {
    return leakRatePerMinute.isPresent() ^ permitRegenerationDuration().isPresent();
  }

  @AssertTrue
  public boolean hasPositiveRegenerationRate() {
    return leakRatePerMillis() > 0;
  }
}
