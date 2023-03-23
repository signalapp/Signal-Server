/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import reactor.core.scheduler.Scheduler;

public class FaultTolerantPubSubConnection<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(FaultTolerantPubSubConnection.class);


  private final String name;
  private final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection;

  private final CircuitBreaker circuitBreaker;
  private final Retry retry;
  private final Retry resubscribeRetry;
  private final Scheduler topologyChangedEventScheduler;

  private final Timer executeTimer;

  public FaultTolerantPubSubConnection(final String name,
      final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection, final CircuitBreaker circuitBreaker,
      final Retry retry, final Retry resubscribeRetry, final Scheduler topologyChangedEventScheduler) {
    this.name = name;
    this.pubSubConnection = pubSubConnection;
    this.circuitBreaker = circuitBreaker;
    this.retry = retry;
    this.resubscribeRetry = resubscribeRetry;
    this.topologyChangedEventScheduler = topologyChangedEventScheduler;

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


  public void subscribeToClusterTopologyChangedEvents(final Runnable eventHandler) {

    usePubSubConnection(connection -> connection.getResources().eventBus().get()
        .filter(event -> event instanceof ClusterTopologyChangedEvent)
        .subscribeOn(topologyChangedEventScheduler)
        .subscribe(event -> {
          logger.info("Got topology change event for {}, resubscribing all keyspace notifications", name);

          resubscribeRetry.executeRunnable(() -> {
            try {
              eventHandler.run();
            } catch (final RuntimeException e) {
              logger.warn("Resubscribe for {} failed", name, e);
              throw e;
            }
          });
        }));
  }

}
