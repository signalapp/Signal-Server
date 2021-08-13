/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;

class CardinalityRateLimiterTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @Test
  void testValidate() {
    final int maxCardinality = 10;
    final CardinalityRateLimiter rateLimiter =
        new CardinalityRateLimiter(REDIS_CLUSTER_EXTENSION.getRedisCluster(), "test", Duration.ofDays(1),
            maxCardinality);

    final String source = "+18005551234";
    int validatedAttempts = 0;
    int blockedAttempts = 0;

    for (int i = 0; i < maxCardinality * 2; i++) {
      try {
        rateLimiter.validate(source, String.valueOf(i), rateLimiter.getDefaultMaxCardinality());
        validatedAttempts++;
      } catch (final RateLimitExceededException e) {
        blockedAttempts++;
      }
    }

    assertTrue(validatedAttempts >= maxCardinality);
    assertTrue(blockedAttempts > 0);

    final String secondSource = "+18005554321";

    try {
      rateLimiter.validate(secondSource, "test", rateLimiter.getDefaultMaxCardinality());
    } catch (final RateLimitExceededException e) {
      fail("New source should not trigger a rate limit exception on first attempted validation");
    }
  }

}
