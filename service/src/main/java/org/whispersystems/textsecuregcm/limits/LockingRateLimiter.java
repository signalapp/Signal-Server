/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.lettuce.core.SetArgs;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

public class LockingRateLimiter extends StaticRateLimiter {

  private static final RateLimitExceededException REUSABLE_RATE_LIMIT_EXCEEDED_EXCEPTION
      = new RateLimitExceededException(Duration.ZERO, true);

  private final Counter counter;


  public LockingRateLimiter(
      final String name,
      final RateLimiterConfig config,
      final FaultTolerantRedisCluster cacheCluster) {
    super(name, config, cacheCluster);
    this.counter = Metrics.counter(name(getClass(), "locked"), "name", name);
  }

  @Override
  public void validate(final String key, final int amount) throws RateLimitExceededException {
    if (!acquireLock(key)) {
      counter.increment();
      throw REUSABLE_RATE_LIMIT_EXCEEDED_EXCEPTION;
    }

    try {
      super.validate(key, amount);
    } finally {
      releaseLock(key);
    }
  }

  private void releaseLock(final String key) {
    cacheCluster.useCluster(connection -> connection.sync().del(getLockName(key)));
  }

  private boolean acquireLock(final String key) {
    return cacheCluster.withCluster(connection -> connection.sync().set(getLockName(key), "L", SetArgs.Builder.nx().ex(10))) != null;
  }

  private String getLockName(final String key) {
    return "leaky_lock::" + name + "::" + key;
  }
}
