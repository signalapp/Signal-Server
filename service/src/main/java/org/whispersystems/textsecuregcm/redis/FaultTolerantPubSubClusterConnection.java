/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.github.resilience4j.retry.Retry;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;

public class FaultTolerantPubSubClusterConnection<K, V> extends AbstractFaultTolerantPubSubConnection<K, V, StatefulRedisClusterPubSubConnection<K, V>> {

  private final Logger logger = LoggerFactory.getLogger(FaultTolerantPubSubClusterConnection.class);

  private final Retry resubscribeRetry;
  private final Scheduler topologyChangedEventScheduler;

  protected FaultTolerantPubSubClusterConnection(final String name,
      final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection,
      final Retry retry,
      final Retry resubscribeRetry,
      final Scheduler topologyChangedEventScheduler) {

    super(name, pubSubConnection, retry);

    pubSubConnection.setNodeMessagePropagation(true);

    this.resubscribeRetry = resubscribeRetry;
    this.topologyChangedEventScheduler = topologyChangedEventScheduler;
  }

  public void subscribeToClusterTopologyChangedEvents(final Runnable eventHandler) {

    usePubSubConnection(connection -> connection.getResources().eventBus().get()
        .filter(event -> event instanceof ClusterTopologyChangedEvent)
        .subscribeOn(topologyChangedEventScheduler)
        .subscribe(event -> {
          logger.info("Got topology change event for {}, resubscribing all keyspace notifications", getName());

          resubscribeRetry.executeRunnable(() -> {
            try {
              eventHandler.run();
            } catch (final RuntimeException e) {
              logger.warn("Resubscribe for {} failed", getName(), e);
              throw e;
            }
          });
        }));
  }
}
