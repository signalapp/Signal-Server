/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;

public class FaultTolerantPubSubConnection<K, V> {

  private final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection;

  private final CircuitBreaker circuitBreaker;
  private final Retry retry;

  private final Timer executeTimer;

  public FaultTolerantPubSubConnection(final String name,
      final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection, final CircuitBreaker circuitBreaker,
      final Retry retry) {
    this.pubSubConnection = pubSubConnection;
    this.circuitBreaker = circuitBreaker;
    this.retry = retry;

    this.pubSubConnection.setNodeMessagePropagation(true);

    this.executeTimer = Metrics.timer(name(getClass(), "execute"), "name", name + "-pubsub");

    CircuitBreakerUtil.registerMetrics(circuitBreaker, FaultTolerantPubSubConnection.class);
  }

  public void usePubSubConnection(final Consumer<StatefulRedisClusterPubSubConnection<K, V>> consumer) {
    try {
      circuitBreaker.executeCheckedRunnable(
          () -> retry.executeRunnable(() -> executeTimer.record(() -> consumer.accept(pubSubConnection))));
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
          return circuitBreaker.executeCheckedSupplier(
              () -> retry.executeCallable(() -> executeTimer.record(() -> function.apply(pubSubConnection))));
        } catch (final Throwable t) {
            if (t instanceof RedisException) {
               throw (RedisException) t;
            } else {
               throw new RedisException(t);
            }
        }
    }
}
