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
import java.util.function.Consumer;

public class FaultTolerantPubSubClusterConnection<K, V> extends AbstractFaultTolerantPubSubConnection<K, V, StatefulRedisClusterPubSubConnection<K, V>> {

  private final Logger logger = LoggerFactory.getLogger(FaultTolerantPubSubClusterConnection.class);

  private final Retry resubscribeRetry;
  private final Scheduler topologyChangedEventScheduler;

  protected FaultTolerantPubSubClusterConnection(final String name,
      final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection,
      final Retry resubscribeRetry,
      final Scheduler topologyChangedEventScheduler) {

    super(name, pubSubConnection);

    pubSubConnection.setNodeMessagePropagation(true);

    this.resubscribeRetry = resubscribeRetry;
    this.topologyChangedEventScheduler = topologyChangedEventScheduler;
  }

  public void subscribeToClusterTopologyChangedEvents(final Consumer<ClusterTopologyChangedEvent> eventHandler) {

    usePubSubConnection(connection -> connection.getResources().eventBus().get()
        .filter(event -> {
          // If we use shared `ClientResources` for multiple clients, we may receive topology change events for clusters
          // other than our own. Filter for clusters that have at least one node in common with our current view of our
          // partitions.
          if (event instanceof ClusterTopologyChangedEvent clusterTopologyChangedEvent) {
            return withPubSubConnection(c -> c.getPartitions().stream().anyMatch(redisClusterNode ->
                clusterTopologyChangedEvent.before().contains(redisClusterNode) ||
                clusterTopologyChangedEvent.after().contains(redisClusterNode)));
          }

          return false;
        })
        .subscribeOn(topologyChangedEventScheduler)
        .subscribe(event -> {
          logger.info("Got topology change event for {}, resubscribing all keyspace notifications", getName());

          resubscribeRetry.executeRunnable(() -> {
            try {
              eventHandler.accept((ClusterTopologyChangedEvent) event);
            } catch (final RuntimeException e) {
              logger.warn("Resubscribe for {} failed", getName(), e);
              throw e;
            }
          });
        }));
  }
}
