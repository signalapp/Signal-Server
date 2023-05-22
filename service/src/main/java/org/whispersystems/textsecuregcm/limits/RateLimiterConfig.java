/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import javax.validation.constraints.AssertTrue;
import java.time.Duration;

public record RateLimiterConfig(int bucketSize, Duration permitRegenerationDuration) {

  public double leakRatePerMillis() {
    return 1.0 / permitRegenerationDuration.toMillis();
  }

  @AssertTrue
  public boolean hasPositiveRegenerationRate() {
    return permitRegenerationDuration.toMillis() > 0;
  }
}
