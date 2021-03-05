/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.CardinalityRateLimitConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import java.time.Duration;
import java.util.Random;

/**
 * A cardinality rate limiter prevents an actor from taking some action if that actor has attempted to take that action
 * on too many targets in a fixed period of time. Behind the scenes, we estimate the target count using a
 * hyper-log-log data structure; as a consequence, the number of targets is an approximation, and this rate limiter
 * should not be used in cases where precise time or target limits are required.
 */
public class CardinalityRateLimiter {

  private final FaultTolerantRedisCluster cacheCluster;

  private final String name;

  private final Duration ttl;
  private final Duration ttlJitter;
  private final int maxCardinality;

  private final Random random = new Random();

  public CardinalityRateLimiter(final FaultTolerantRedisCluster cacheCluster, final String name, final Duration ttl, final Duration ttlJitter, final int maxCardinality) {
    this.cacheCluster = cacheCluster;

    this.name = name;

    this.ttl = ttl;
    this.ttlJitter = ttlJitter;
    this.maxCardinality = maxCardinality;
  }

  public void validate(final String key, final String target) throws RateLimitExceededException {
    final String hllKey = getHllKey(key);

    final boolean rateLimitExceeded = cacheCluster.withCluster(connection -> {
      final boolean changed = connection.sync().pfadd(hllKey, target) == 1;
      final long cardinality = connection.sync().pfcount(hllKey);

      final boolean mayNeedExpiration = changed && cardinality == 1;

      // If the set already existed, we can assume it already had an expiration time and can save a round trip by
      // skipping the ttl check.
      if (mayNeedExpiration && connection.sync().ttl(hllKey) == -1) {
        final long expireSeconds = ttl.plusSeconds(random.nextInt((int) ttlJitter.toSeconds())).toSeconds();
        connection.sync().expire(hllKey, expireSeconds);
      }

      return changed && cardinality > maxCardinality;
    });

    if (rateLimitExceeded) {
      // Using the TTL as the "retry after" time isn't EXACTLY right, but it's a reasonable approximation
      throw new RateLimitExceededException(ttl);
    }
  }

  private String getHllKey(final String key) {
    return "hll_rate_limit::" + name + "::" + key;
  }

  public Duration getTtl() {
    return ttl;
  }

  public Duration getTtlJitter() {
    return ttlJitter;
  }

  public int getMaxCardinality() {
    return maxCardinality;
  }

  public boolean hasConfiguration(final CardinalityRateLimitConfiguration configuration) {
    return maxCardinality == configuration.getMaxCardinality() &&
        ttl.equals(configuration.getTtl()) &&
        ttlJitter.equals(configuration.getTtlJitter());
  }
}
