/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.pubsub.api.sync.RedisClusterPubSubCommands;
import io.lettuce.core.event.Event;
import io.lettuce.core.event.EventBus;
import io.lettuce.core.resource.ClientResources;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.publisher.TestPublisher;

class FaultTolerantPubSubClusterConnectionTest {

  private StatefulRedisClusterPubSubConnection<String, String> pubSubConnection;
  private RedisClusterPubSubCommands<String, String> pubSubCommands;
  private FaultTolerantPubSubClusterConnection<String, String> faultTolerantPubSubConnection;

  private TestPublisher<Event> eventPublisher;

  private Consumer<ClusterTopologyChangedEvent> resubscribe;

  private AtomicInteger resubscribeCounter;
  private CountDownLatch resubscribeFailure;
  private CountDownLatch resubscribeSuccess;

  private RedisClusterNode nodeInCluster;

  @SuppressWarnings("unchecked")
  @BeforeEach
  public void setUp() {
    pubSubConnection = mock(StatefulRedisClusterPubSubConnection.class);
    pubSubCommands = mock(RedisClusterPubSubCommands.class);
    nodeInCluster = mock(RedisClusterNode.class);

    final ClientResources clientResources = mock(ClientResources.class);

    final Partitions partitions = new Partitions();
    partitions.add(nodeInCluster);

    when(pubSubConnection.sync()).thenReturn(pubSubCommands);
    when(pubSubConnection.getResources()).thenReturn(clientResources);
    when(pubSubConnection.getPartitions()).thenReturn(partitions);

    final RetryConfiguration retryConfiguration = new RetryConfiguration();
    retryConfiguration.setMaxAttempts(3);
    retryConfiguration.setWaitDuration(10);

    final RetryConfig resubscribeRetryConfiguration = RetryConfig.custom()
        .maxAttempts(Integer.MAX_VALUE)
        .intervalFunction(IntervalFunction.ofExponentialBackoff(5))
        .build();
    final Retry resubscribeRetry = Retry.of("test-resubscribe", resubscribeRetryConfiguration);

    faultTolerantPubSubConnection = new FaultTolerantPubSubClusterConnection<>("test", pubSubConnection,
        resubscribeRetry, Schedulers.newSingle("test"));

    eventPublisher = TestPublisher.createCold();

    final EventBus eventBus = mock(EventBus.class);
    when(clientResources.eventBus()).thenReturn(eventBus);

    final Flux<Event> eventFlux = Flux.from(eventPublisher);
    when(eventBus.get()).thenReturn(eventFlux);

    resubscribeCounter = new AtomicInteger();

    resubscribe = event -> {
      try {
        resubscribeCounter.incrementAndGet();
        pubSubConnection.sync().nodes((ignored) -> true);
        resubscribeSuccess.countDown();
      } catch (final RuntimeException e) {
        resubscribeFailure.countDown();
        throw e;
      }
    };

    resubscribeSuccess = new CountDownLatch(1);
    resubscribeFailure = new CountDownLatch(1);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testSubscribeToClusterTopologyChangedEvents() throws Exception {

    when(pubSubConnection.sync())
        .thenThrow(new RedisException("Cluster unavailable"));

    eventPublisher.next(new ClusterTopologyChangedEvent(List.of(nodeInCluster), List.of(nodeInCluster)));

    faultTolerantPubSubConnection.subscribeToClusterTopologyChangedEvents(resubscribe);

    assertTrue(resubscribeFailure.await(1, TimeUnit.SECONDS));

    // simulate cluster recovery - no more exceptions, run the retry
    reset(pubSubConnection);
    clearInvocations(pubSubCommands);
    when(pubSubConnection.sync())
        .thenReturn(pubSubCommands);

    assertTrue(resubscribeSuccess.await(1, TimeUnit.SECONDS));

    assertTrue(resubscribeCounter.get() >= 2, String.format("resubscribe called %d times", resubscribeCounter.get()));
    verify(pubSubCommands).nodes(any());
  }

  @Test
  void testFilterClusterTopologyChangeEvents() throws InterruptedException {
    final CountDownLatch topologyEventLatch = new CountDownLatch(1);

    faultTolerantPubSubConnection.subscribeToClusterTopologyChangedEvents(event -> topologyEventLatch.countDown());

    final RedisClusterNode nodeFromDifferentCluster = mock(RedisClusterNode.class);

    eventPublisher.next(new ClusterTopologyChangedEvent(List.of(nodeFromDifferentCluster), List.of(nodeFromDifferentCluster)));

    assertFalse(topologyEventLatch.await(1, TimeUnit.SECONDS));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testMultipleEventsWithPendingRetries() throws Exception {
    // more complicated scenario: multiple events while retries are pending

    // cluster is down
    when(pubSubConnection.sync())
        .thenThrow(new RedisException("Cluster unavailable"));

    // publish multiple topology changed events
    final ClusterTopologyChangedEvent clusterTopologyChangedEvent =
        new ClusterTopologyChangedEvent(List.of(nodeInCluster), List.of(nodeInCluster));

    eventPublisher.next(clusterTopologyChangedEvent);
    eventPublisher.next(clusterTopologyChangedEvent);
    eventPublisher.next(clusterTopologyChangedEvent);
    eventPublisher.next(clusterTopologyChangedEvent);

    faultTolerantPubSubConnection.subscribeToClusterTopologyChangedEvents(resubscribe);

    assertTrue(resubscribeFailure.await(1, TimeUnit.SECONDS));

    // simulate cluster recovery - no more exceptions, run the retry
    reset(pubSubConnection);
    clearInvocations(pubSubCommands);
    when(pubSubConnection.sync())
        .thenReturn(pubSubCommands);

    assertTrue(resubscribeSuccess.await(1, TimeUnit.SECONDS));

    verify(pubSubCommands, atLeastOnce()).nodes(any());
  }
}
