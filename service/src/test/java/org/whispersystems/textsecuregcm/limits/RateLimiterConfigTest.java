/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalDouble;

import static org.junit.jupiter.api.Assertions.*;

class RateLimiterConfigTest {

  @Test
  void leakRatePerMillis() {
    assertEquals(0.001, new RateLimiterConfig(1, OptionalDouble.of(60), Optional.empty()).leakRatePerMillis());
    assertEquals(0.001, new RateLimiterConfig(1, OptionalDouble.empty(), Optional.of(Duration.ofSeconds(1))).leakRatePerMillis());
  }

  @Test
  void hasExactlyOneRegenerationRate() {
    assertTrue(new RateLimiterConfig(1, OptionalDouble.of(1), Optional.empty()).hasExactlyOneRegenerationRate());
    assertTrue(new RateLimiterConfig(1, OptionalDouble.empty(), Optional.of(Duration.ofSeconds(1))).hasExactlyOneRegenerationRate());
    assertFalse(new RateLimiterConfig(1, OptionalDouble.of(1), Optional.of(Duration.ofSeconds(1))).hasExactlyOneRegenerationRate());
    assertFalse(new RateLimiterConfig(1, OptionalDouble.empty(), Optional.empty()).hasExactlyOneRegenerationRate());
  }

  @Test
  void isRegenerationRatePositive() {
    assertTrue(new RateLimiterConfig(1, OptionalDouble.of(1), Optional.empty()).hasPositiveRegenerationRate());
    assertTrue(new RateLimiterConfig(1, OptionalDouble.empty(), Optional.of(Duration.ofSeconds(1))).hasPositiveRegenerationRate());
    assertFalse(new RateLimiterConfig(1, OptionalDouble.of(-1), Optional.empty()).hasPositiveRegenerationRate());
    assertFalse(new RateLimiterConfig(1, OptionalDouble.empty(), Optional.of(Duration.ofSeconds(-1))).hasPositiveRegenerationRate());
    assertFalse(new RateLimiterConfig(1, OptionalDouble.of(1 / 10), Optional.empty()).hasPositiveRegenerationRate());
  }
}
