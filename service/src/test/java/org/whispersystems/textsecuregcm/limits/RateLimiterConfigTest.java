/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class RateLimiterConfigTest {

  @Test
  void leakRatePerMillis() {
    assertEquals(0.001, new RateLimiterConfig(1, Duration.ofSeconds(1), false).leakRatePerMillis());
    assertEquals(1e6, new RateLimiterConfig(1, Duration.ofNanos(1), false).leakRatePerMillis());
  }

  @Test
  void isRegenerationRatePositive() {
    assertTrue(new RateLimiterConfig(1, Duration.ofSeconds(1), false).hasPositiveRegenerationRate());
    assertTrue(new RateLimiterConfig(1, Duration.ofNanos(1), false).hasPositiveRegenerationRate());
    assertFalse(new RateLimiterConfig(1, Duration.ZERO, false).hasPositiveRegenerationRate());
    assertFalse(new RateLimiterConfig(1, Duration.ofSeconds(-1), false).hasPositiveRegenerationRate());
  }
}
