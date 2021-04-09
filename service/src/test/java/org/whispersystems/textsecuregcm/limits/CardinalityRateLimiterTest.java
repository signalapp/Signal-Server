/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;

import java.time.Duration;

import static org.junit.Assert.*;

public class CardinalityRateLimiterTest extends AbstractRedisClusterTest {

    @Before
    public void setUp() throws Exception {
      super.setUp();
    }

    @After
    public void tearDown() throws Exception {
      super.tearDown();
    }

    @Test
    public void testValidate() {
      final int maxCardinality = 10;
      final CardinalityRateLimiter rateLimiter = new CardinalityRateLimiter(getRedisCluster(), "test", Duration.ofDays(1), Duration.ofDays(1), maxCardinality);

      final String source = "+18005551234";
      int validatedAttempts = 0;
      int blockedAttempts = 0;

      for (int i = 0; i < maxCardinality * 2; i++) {
        try {
          rateLimiter.validate(source, String.valueOf(i));
          validatedAttempts++;
        } catch (final RateLimitExceededException e) {
          blockedAttempts++;
        }
      }

      assertTrue(validatedAttempts >= maxCardinality);
      assertTrue(blockedAttempts > 0);

      final String secondSource = "+18005554321";

      try {
        rateLimiter.validate(secondSource, "test");
      } catch (final RateLimitExceededException e) {
        fail("New source should not trigger a rate limit exception on first attempted validation");
      }
    }
}
