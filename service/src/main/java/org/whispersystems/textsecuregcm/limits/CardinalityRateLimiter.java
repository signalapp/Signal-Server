/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import java.time.Duration;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.CardinalityRateLimitConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

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
  private final int defaultMaxCardinality;

  public CardinalityRateLimiter(final FaultTolerantRedisCluster cacheCluster, final String name, final Duration ttl, final int defaultMaxCardinality) {
    this.cacheCluster = cacheCluster;

    this.name = name;

    this.ttl = ttl;
    this.defaultMaxCardinality = defaultMaxCardinality;
  }

  public void validate(final String key, final String target, final int maxCardinality) throws RateLimitExceededException {

    final boolean rateLimitExceeded = cacheCluster.withCluster(connection -> {
      final String hllKey = getHllKey(key);

      final boolean changed = connection.sync().pfadd(hllKey, target) == 1;
      final long cardinality = connection.sync().pfcount(hllKey);

      final boolean mayNeedExpiration = changed && cardinality == 1;

      // If the set already existed, we can assume it already had an expiration time and can save a round trip by
      // skipping the ttl check.
      if (mayNeedExpiration && connection.sync().ttl(hllKey) == -1) {
        connection.sync().expire(hllKey, ttl.toSeconds());
      }

      return changed && cardinality > maxCardinality;
    });

    if (rateLimitExceeded) {
      throw new RateLimitExceededException(Duration.ofSeconds(getRemainingTtl(key)));
    }
  }

  private String getHllKey(final String key) {
    return "hll_rate_limit::" + name + "::" + key;
  }

  public Duration getInitialTtl() {
    return ttl;
  }

  public long getRemainingTtl(final String key) {
    return cacheCluster.withCluster(connection -> connection.sync().ttl(getHllKey(key)));
  }

  public int getDefaultMaxCardinality() {
    return defaultMaxCardinality;
  }

  public boolean hasConfiguration(final CardinalityRateLimitConfiguration configuration) {
    return defaultMaxCardinality == configuration.getMaxCardinality() && ttl.equals(configuration.getTtl());
  }

}
