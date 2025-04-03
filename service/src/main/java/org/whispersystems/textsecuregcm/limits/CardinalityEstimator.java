/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.Util;

import static io.lettuce.core.ExpireArgs.Builder.nx;

/**
 * Estimate the number of unique items seen over a configurable period and update a metric
 */
public class CardinalityEstimator {

  private volatile long uniqueElementCount;
  private final FaultTolerantRedisClusterClient redisCluster;
  private final String hllName;
  private final Duration period;

  public CardinalityEstimator(final FaultTolerantRedisClusterClient redisCluster, final String name, final Duration period) {
    this.redisCluster = redisCluster;
    this.hllName = "cardinality_estimator::" + name;
    this.period = period;
    Metrics.gauge(
        MetricsUtil.name(getClass(), "unique"),
        Tags.of("metricName", name),
        this,
        obj -> obj.uniqueElementCount);
  }

  public void add(final String element) {
    addAsync(element).toCompletableFuture().join();
  }

  public CompletionStage<Void> addAsync(final String element) {
    return redisCluster.withCluster(connection -> connection.async()
        .pfadd(hllName, element)
        .thenCompose(modCount -> {
          if (modCount == 0) {
            // The hll hasn't changed - return our current view of cardinality
            return CompletableFuture.completedFuture(uniqueElementCount);
          }

          return connection.async().pfcount(hllName);
        })
        .thenCompose(newUniqueElementCount -> {
          uniqueElementCount = newUniqueElementCount;
          return connection.async().expire(hllName, period, nx()).thenRun(Util.NOOP);
        }));
  }

  @VisibleForTesting
  long estimate() {
    return this.uniqueElementCount;
  }
}
