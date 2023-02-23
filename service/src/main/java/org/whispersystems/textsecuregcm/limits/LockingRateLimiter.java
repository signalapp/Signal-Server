/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import io.lettuce.core.SetArgs;
import java.time.Duration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.Constants;

public class LockingRateLimiter extends StaticRateLimiter {

  private static final RateLimitExceededException REUSABLE_RATE_LIMIT_EXCEEDED_EXCEPTION
      = new RateLimitExceededException(Duration.ZERO, true);

  private final Meter meter;


  public LockingRateLimiter(
      final String name,
      final RateLimiterConfig config,
      final FaultTolerantRedisCluster cacheCluster) {
    super(name, config, cacheCluster);
    final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
    this.meter = metricRegistry.meter(name(getClass(), name, "locked"));
  }

  @Override
  public void validate(final String key, final int amount) throws RateLimitExceededException {
    if (!acquireLock(key)) {
      meter.mark();
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
