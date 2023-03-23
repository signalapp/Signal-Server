/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.pubsub.api.sync.RedisClusterPubSubCommands;
import io.lettuce.core.event.Event;
import io.lettuce.core.event.EventBus;
import io.lettuce.core.resource.ClientResources;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.publisher.TestPublisher;

class FaultTolerantPubSubConnectionTest {

  private StatefulRedisClusterPubSubConnection<String, String> pubSubConnection;
  private RedisClusterPubSubCommands<String, String> pubSubCommands;
  private FaultTolerantPubSubConnection<String, String> faultTolerantPubSubConnection;


  @SuppressWarnings("unchecked")
  @BeforeEach
  public void setUp() {
    pubSubConnection = mock(StatefulRedisClusterPubSubConnection.class);

    pubSubCommands = mock(RedisClusterPubSubCommands.class);

    when(pubSubConnection.sync()).thenReturn(pubSubCommands);

    final CircuitBreakerConfiguration breakerConfiguration = new CircuitBreakerConfiguration();
    breakerConfiguration.setFailureRateThreshold(100);
    breakerConfiguration.setSlidingWindowSize(1);
    breakerConfiguration.setSlidingWindowMinimumNumberOfCalls(1);
    breakerConfiguration.setWaitDurationInOpenStateInSeconds(Integer.MAX_VALUE);

    final RetryConfiguration retryConfiguration = new RetryConfiguration();
    retryConfiguration.setMaxAttempts(3);
    retryConfiguration.setWaitDuration(10);

    final CircuitBreaker circuitBreaker = CircuitBreaker.of("test", breakerConfiguration.toCircuitBreakerConfig());
    final Retry retry = Retry.of("test", retryConfiguration.toRetryConfig());

    final RetryConfig resubscribeRetryConfiguration = RetryConfig.custom()
        .maxAttempts(Integer.MAX_VALUE)
        .intervalFunction(IntervalFunction.ofExponentialBackoff(5))
        .build();
    final Retry resubscribeRetry = Retry.of("test-resubscribe", resubscribeRetryConfiguration);

    faultTolerantPubSubConnection = new FaultTolerantPubSubConnection<>("test", pubSubConnection, circuitBreaker,
        retry, resubscribeRetry, Schedulers.newSingle("test"));
  }

  @Test
  void testBreaker() {
    when(pubSubCommands.get(anyString()))
        .thenReturn("value")
        .thenThrow(new RuntimeException("Badness has ensued."));

    assertEquals("value",
        faultTolerantPubSubConnection.withPubSubConnection(connection -> connection.sync().get("key")));

    assertThrows(RedisException.class,
        () -> faultTolerantPubSubConnection.withPubSubConnection(connection -> connection.sync().get("OH NO")));

    final RedisException redisException = assertThrows(RedisException.class,
        () -> faultTolerantPubSubConnection.withPubSubConnection(connection -> connection.sync().get("OH NO")));

    assertTrue(redisException.getCause() instanceof CallNotPermittedException);
  }

  @Test
  void testRetry() {
    when(pubSubCommands.get(anyString()))
        .thenThrow(new RedisCommandTimeoutException())
        .thenThrow(new RedisCommandTimeoutException())
        .thenReturn("value");

    assertEquals("value",
        faultTolerantPubSubConnection.withPubSubConnection(connection -> connection.sync().get("key")));

    when(pubSubCommands.get(anyString()))
        .thenThrow(new RedisCommandTimeoutException())
        .thenThrow(new RedisCommandTimeoutException())
        .thenThrow(new RedisCommandTimeoutException())
        .thenReturn("value");

    assertThrows(RedisCommandTimeoutException.class,
        () -> faultTolerantPubSubConnection.withPubSubConnection(connection -> connection.sync().get("key")));
  }

  @Nested
  class ClusterTopologyChangedEventTest {

    private TestPublisher<Event> eventPublisher;

    private Runnable resubscribe;

    private AtomicInteger resubscribeCounter;
    private CountDownLatch resubscribeFailure;
    private CountDownLatch resubscribeSuccess;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
      // ignore inherited stubbing
      reset(pubSubConnection);

      eventPublisher = TestPublisher.createCold();

      final ClientResources clientResources = mock(ClientResources.class);
      when(pubSubConnection.getResources())
          .thenReturn(clientResources);
      final EventBus eventBus = mock(EventBus.class);
      when(clientResources.eventBus())
          .thenReturn(eventBus);

      final Flux<Event> eventFlux = Flux.from(eventPublisher);
      when(eventBus.get()).thenReturn(eventFlux);

      resubscribeCounter = new AtomicInteger();

      resubscribe = () -> {
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

      eventPublisher.next(new ClusterTopologyChangedEvent(Collections.emptyList(), Collections.emptyList()));

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
    @SuppressWarnings("unchecked")
    void testMultipleEventsWithPendingRetries() throws Exception {
      // more complicated scenario: multiple events while retries are pending

      // cluster is down
      when(pubSubConnection.sync())
          .thenThrow(new RedisException("Cluster unavailable"));

      // publish multiple topology changed events
      eventPublisher.next(new ClusterTopologyChangedEvent(Collections.emptyList(), Collections.emptyList()));
      eventPublisher.next(new ClusterTopologyChangedEvent(Collections.emptyList(), Collections.emptyList()));
      eventPublisher.next(new ClusterTopologyChangedEvent(Collections.emptyList(), Collections.emptyList()));
      eventPublisher.next(new ClusterTopologyChangedEvent(Collections.emptyList(), Collections.emptyList()));

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

}
