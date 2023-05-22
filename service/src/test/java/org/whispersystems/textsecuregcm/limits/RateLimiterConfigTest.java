/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class RateLimiterConfigTest {

  @Test
  void leakRatePerMillis() {
    assertEquals(0.001, new RateLimiterConfig(1, Duration.ofSeconds(1)).leakRatePerMillis());
  }

  @Test
  void isRegenerationRatePositive() {
    assertTrue(new RateLimiterConfig(1, Duration.ofSeconds(1)).hasPositiveRegenerationRate());
    assertFalse(new RateLimiterConfig(1, Duration.ZERO).hasPositiveRegenerationRate());
    assertFalse(new RateLimiterConfig(1, Duration.ofSeconds(-1)).hasPositiveRegenerationRate());
  }
}
