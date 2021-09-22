/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import java.util.function.Consumer;
import java.util.function.Function;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.util.Constants;

public class FaultTolerantPubSubConnection<K, V> {

    private final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection;

    private final CircuitBreaker circuitBreaker;
    private final Retry          retry;

    private final Timer         executeTimer;

    public FaultTolerantPubSubConnection(final String name, final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection, final CircuitBreaker circuitBreaker, final Retry retry) {
      this.pubSubConnection = pubSubConnection;
      this.circuitBreaker = circuitBreaker;
      this.retry = retry;

      this.pubSubConnection.setNodeMessagePropagation(true);

      final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
      this.executeTimer = metricRegistry.timer(name(getClass(), name + "-pubsub", "execute"));

      CircuitBreakerUtil.registerMetrics(metricRegistry, circuitBreaker, FaultTolerantPubSubConnection.class);
    }

    public void usePubSubConnection(final Consumer<StatefulRedisClusterPubSubConnection<K, V>> consumer) {
        try {
            circuitBreaker.executeCheckedRunnable(() -> retry.executeRunnable(() -> {
                try (final Timer.Context ignored = executeTimer.time()) {
                    consumer.accept(pubSubConnection);
                }
            }));
        } catch (final Throwable t) {
            if (t instanceof RedisException) {
               throw (RedisException) t;
            } else {
               throw new RedisException(t);
            }
        }
    }

    public <T> T withPubSubConnection(final Function<StatefulRedisClusterPubSubConnection<K, V>, T> function) {
        try {
            return circuitBreaker.executeCheckedSupplier(() -> retry.executeCallable(() -> {
                try (final Timer.Context ignored = executeTimer.time()) {
                    return function.apply(pubSubConnection);
                }
            }));
        } catch (final Throwable t) {
            if (t instanceof RedisException) {
               throw (RedisException) t;
            } else {
               throw new RedisException(t);
            }
        }
    }
}
