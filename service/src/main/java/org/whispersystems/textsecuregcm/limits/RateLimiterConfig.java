/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

public record RateLimiterConfig(int bucketSize, double leakRatePerMinute) {

  public double leakRatePerMillis() {
    return leakRatePerMinute / (60.0 * 1000.0);
  }
}
