/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.limits;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.requireNonNull;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class StaticRateLimiter implements RateLimiter {

  private static final Logger logger = LoggerFactory.getLogger(StaticRateLimiter.class);

  protected final String name;

  private final RateLimiterConfig config;

  protected final FaultTolerantRedisCluster cacheCluster;

  private final Meter meter;

  private final Timer validateTimer;


  public StaticRateLimiter(
      final String name,
      final RateLimiterConfig config,
      final FaultTolerantRedisCluster cacheCluster) {
    final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
    this.name = requireNonNull(name);
    this.config = requireNonNull(config);
    this.cacheCluster = requireNonNull(cacheCluster);
    this.meter = metricRegistry.meter(name(getClass(), name, "exceeded"));
    this.validateTimer = metricRegistry.timer(name(getClass(), name, "validate"));
  }

  @Override
  public void validate(final String key, final int amount) throws RateLimitExceededException {
    try (final Timer.Context ignored = validateTimer.time()) {
      final LeakyBucket bucket = getBucket(key);
      if (bucket.add(amount)) {
        setBucket(key, bucket);
      } else {
        meter.mark();
        throw new RateLimitExceededException(bucket.getTimeUntilSpaceAvailable(amount), true);
      }
    }
  }

  @Override
  public boolean hasAvailablePermits(final String key, final int permits) {
    return getBucket(key).getTimeUntilSpaceAvailable(permits).equals(Duration.ZERO);
  }

  @Override
  public void clear(final String key) {
    cacheCluster.useCluster(connection -> connection.sync().del(getBucketName(key)));
  }

  @Override
  public RateLimiterConfig config() {
    return config;
  }

  private void setBucket(final String key, final LeakyBucket bucket) {
    try {
      final String serialized = bucket.serialize(SystemMapper.jsonMapper());
      cacheCluster.useCluster(connection -> connection.sync().setex(
          getBucketName(key),
          (int) Math.ceil((config.bucketSize() / config.leakRatePerMillis()) / 1000),
          serialized));
    } catch (final JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private LeakyBucket getBucket(final String key) {
    try {
      final String serialized = cacheCluster.withCluster(connection -> connection.sync().get(getBucketName(key)));

      if (serialized != null) {
        return LeakyBucket.fromSerialized(SystemMapper.jsonMapper(), serialized);
      }
    } catch (final IOException e) {
      logger.warn("Deserialization error", e);
    }

    return new LeakyBucket(config.bucketSize(), config.leakRatePerMillis());
  }

  private String getBucketName(final String key) {
    return "leaky_bucket::" + name + "::" + key;
  }
}
