/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.limits;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.RateLimitConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class RateLimiter {

  private final Logger       logger = LoggerFactory.getLogger(RateLimiter.class);
  private final ObjectMapper mapper = SystemMapper.getMapper();

  private   final Meter                     meter;
  private   final Timer                     validateTimer;
  protected final FaultTolerantRedisCluster cacheCluster;
  protected final String                    name;
  private   final int                       bucketSize;
  private   final double                    leakRatePerMinute;
  private   final double                    leakRatePerMillis;

  public RateLimiter(FaultTolerantRedisCluster cacheCluster, String name, int bucketSize, double leakRatePerMinute)
  {
    MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

    this.meter                  = metricRegistry.meter(name(getClass(), name, "exceeded"));
    this.validateTimer          = metricRegistry.timer(name(getClass(), name, "validate"));
    this.cacheCluster           = cacheCluster;
    this.name                   = name;
    this.bucketSize             = bucketSize;
    this.leakRatePerMinute      = leakRatePerMinute;
    this.leakRatePerMillis      = leakRatePerMinute / (60.0 * 1000.0);
  }

  public void validate(String key, int amount) throws RateLimitExceededException {
    try (final Timer.Context ignored = validateTimer.time()) {
      LeakyBucket bucket = getBucket(key);

      if (bucket.add(amount)) {
        setBucket(key, bucket);
      } else {
        meter.mark();
        throw new RateLimitExceededException(bucket.getTimeUntilSpaceAvailable(amount));
      }
    }
  }

  public void validate(final UUID accountUuid) throws RateLimitExceededException {
    validate(accountUuid.toString());
  }

  public void validate(final UUID sourceAccountUuid, final UUID destinationAccountUuid)
      throws RateLimitExceededException {

    validate(sourceAccountUuid.toString() + "__" + destinationAccountUuid.toString());
  }

  public void validate(String key) throws RateLimitExceededException {
    validate(key, 1);
  }

  public boolean hasAvailablePermits(final UUID accountUuid, final int permits) {
    return hasAvailablePermits(accountUuid.toString(), permits);
  }

  public boolean hasAvailablePermits(final String key, final int permits) {
    return getBucket(key).getTimeUntilSpaceAvailable(permits).equals(Duration.ZERO);
  }

  public void clear(final UUID accountUuid) {
    clear(accountUuid.toString());
  }

  public void clear(String key) {
    cacheCluster.useCluster(connection -> connection.sync().del(getBucketName(key)));
  }

  public int getBucketSize() {
    return bucketSize;
  }

  public double getLeakRatePerMinute() {
    return leakRatePerMinute;
  }

  private void setBucket(String key, LeakyBucket bucket) {

    try {
      final String serialized = bucket.serialize(mapper);

      cacheCluster.useCluster(connection -> connection.sync().setex(getBucketName(key), (int) Math.ceil((bucketSize / leakRatePerMillis) / 1000), serialized));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private LeakyBucket getBucket(String key) {
    try {
      final String serialized = cacheCluster.withCluster(connection -> connection.sync().get(getBucketName(key)));

      if (serialized != null) {
        return LeakyBucket.fromSerialized(mapper, serialized);
      }
    } catch (IOException e) {
      logger.warn("Deserialization error", e);
    }

    return new LeakyBucket(bucketSize, leakRatePerMillis);
  }

  private String getBucketName(String key) {
    return "leaky_bucket::" + name + "::" + key;
  }

  public boolean hasConfiguration(final RateLimitConfiguration configuration) {
    return bucketSize == configuration.getBucketSize() && leakRatePerMinute == configuration.getLeakRatePerMinute();
  }
}
