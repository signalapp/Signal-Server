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
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.Util;

/**
 * Estimate the number of unique items seen over a configurable period and update a metric
 */
public class CardinalityEstimator {

  private volatile double uniqueElementCount;
  private final FaultTolerantRedisCluster redisCluster;
  private final String hllName;
  private final Duration period;

  public CardinalityEstimator(final FaultTolerantRedisCluster redisCluster, final String name, final Duration period) {
    this.redisCluster = redisCluster;
    this.hllName = "cardinality_estimator::" + name;
    this.period = period;
    Metrics.gauge(
        MetricsUtil.name(getClass(), "unique"),
        Tags.of("metricName", name),
        this,
        obj -> obj.uniqueElementCount);
  }

  public void add(String element) {
    addAsync(element).toCompletableFuture().join();
  }

  public CompletionStage<Void> addAsync(String element) {
    return redisCluster.withCluster(connection -> connection.async()
        .pfadd(hllName, element)
        .thenCompose(modCount -> {
          if (modCount == 0) {
            return CompletableFuture.completedFuture(false);
          }

          // The hll changed - update our local view of the cardinality, and
          // initialize the TTL if required
          return connection.async()
              .pfcount(hllName)
              .thenCompose(count -> {
                uniqueElementCount = count;
                // check if this is a new hll with no TTL set
                return connection.async().ttl(hllName).thenApply(ttl -> ttl == -1);
              });
        })
        .thenCompose(isNewHll -> {
          if (!isNewHll) {
            return CompletableFuture.completedFuture(null);
          }

          // If this is a new hll, we need to set the TTL. This could be
          // a single atomic op in redis 7.x with EXPIRE NX
          return connection.async().expire(hllName, period).thenRun(Util.NOOP);
        }));
  }

  @VisibleForTesting
  long estimate() {
    return (long) this.uniqueElementCount;
  }
}
