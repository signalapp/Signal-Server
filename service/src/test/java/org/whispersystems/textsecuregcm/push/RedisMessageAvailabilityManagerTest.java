/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.api.async.RedisClusterPubSubAsyncCommands;
import io.lettuce.core.cluster.pubsub.api.sync.RedisClusterPubSubCommands;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.MockRedisFuture;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;

@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RedisMessageAvailabilityManagerTest {

  private RedisMessageAvailabilityManager localEventManager;
  private RedisMessageAvailabilityManager remoteEventManager;

  private static ExecutorService webSocketConnectionEventExecutor;
  private static ExecutorService asyncOperationQueueingExecutor;

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private static class MessageAvailabilityAdapter implements MessageAvailabilityListener {

    @Override
    public void handleNewMessageAvailable() {
    }

    @Override
    public void handleMessagesPersisted() {
    }

    @Override
    public void handleConflictingMessageConsumer() {
    }
  }

  @BeforeAll
  static void setUpBeforeAll() {
    webSocketConnectionEventExecutor = Executors.newVirtualThreadPerTaskExecutor();
    asyncOperationQueueingExecutor = Executors.newSingleThreadExecutor();
  }

  @BeforeEach
  void setUp() {
    localEventManager = new RedisMessageAvailabilityManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        webSocketConnectionEventExecutor,
        asyncOperationQueueingExecutor);

    remoteEventManager = new RedisMessageAvailabilityManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        webSocketConnectionEventExecutor,
        asyncOperationQueueingExecutor);

    localEventManager.start();
    remoteEventManager.start();
  }

  @AfterEach
  void tearDown() {
    localEventManager.stop();
    remoteEventManager.stop();
  }

  @AfterAll
  static void tearDownAfterAll() {
    webSocketConnectionEventExecutor.shutdown();
    asyncOperationQueueingExecutor.shutdown();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void handleClientConnected(final boolean displaceRemotely) throws InterruptedException {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final AtomicBoolean firstListenerDisplaced = new AtomicBoolean(false);

    final AtomicBoolean secondListenerDisplaced = new AtomicBoolean(false);

    localEventManager.handleClientConnected(accountIdentifier, deviceId, new MessageAvailabilityAdapter() {
      @Override
      public void handleConflictingMessageConsumer() {
        synchronized (firstListenerDisplaced) {
          firstListenerDisplaced.set(true);
          firstListenerDisplaced.notifyAll();
        }
      }
    }).toCompletableFuture().join();

    assertFalse(firstListenerDisplaced.get());
    assertFalse(secondListenerDisplaced.get());

    final RedisMessageAvailabilityManager displacingManager =
        displaceRemotely ? remoteEventManager : localEventManager;

    displacingManager.handleClientConnected(accountIdentifier, deviceId, new MessageAvailabilityAdapter() {
      @Override
      public void handleConflictingMessageConsumer() {
        secondListenerDisplaced.set(true);
      }
    }).toCompletableFuture().join();

    synchronized (firstListenerDisplaced) {
      while (!firstListenerDisplaced.get()) {
        firstListenerDisplaced.wait();
      }
    }

    assertTrue(firstListenerDisplaced.get());
    assertFalse(secondListenerDisplaced.get());
  }

  @Test
  void isLocallyPresent() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    assertFalse(localEventManager.isLocallyPresent(accountIdentifier, deviceId));
    assertFalse(remoteEventManager.isLocallyPresent(accountIdentifier, deviceId));

    localEventManager.handleClientConnected(accountIdentifier, deviceId, new MessageAvailabilityAdapter())
        .toCompletableFuture()
        .join();

    assertTrue(localEventManager.isLocallyPresent(accountIdentifier, deviceId));
    assertFalse(remoteEventManager.isLocallyPresent(accountIdentifier, deviceId));

    localEventManager.handleClientDisconnected(accountIdentifier, deviceId)
        .toCompletableFuture()
        .join();

    assertFalse(localEventManager.isLocallyPresent(accountIdentifier, deviceId));
    assertFalse(remoteEventManager.isLocallyPresent(accountIdentifier, deviceId));
  }

  @Test
  void resubscribe() {
    @SuppressWarnings("unchecked") final RedisClusterPubSubCommands<byte[], byte[]> pubSubCommands =
        mock(RedisClusterPubSubCommands.class);

    @SuppressWarnings("unchecked") final RedisClusterPubSubAsyncCommands<byte[], byte[]> pubSubAsyncCommands =
        mock(RedisClusterPubSubAsyncCommands.class);

    when(pubSubAsyncCommands.ssubscribe(any())).thenReturn(MockRedisFuture.completedFuture(null));

    final FaultTolerantRedisClusterClient clusterClient = RedisClusterHelper.builder()
        .binaryPubSubCommands(pubSubCommands)
        .binaryPubSubAsyncCommands(pubSubAsyncCommands)
        .build();

    final RedisMessageAvailabilityManager eventManager = new RedisMessageAvailabilityManager(
        clusterClient,
        Runnable::run,
        Runnable::run);

    eventManager.start();

    final UUID firstAccountIdentifier = UUID.randomUUID();
    final byte firstDeviceId = Device.PRIMARY_ID;
    final int firstSlot = SlotHash.getSlot(RedisMessageAvailabilityManager.getClientEventChannel(firstAccountIdentifier, firstDeviceId));

    final UUID secondAccountIdentifier;
    final byte secondDeviceId = firstDeviceId + 1;

    // Make sure that the two subscriptions wind up in different slots
    {
      UUID candidateIdentifier;

      do {
        candidateIdentifier = UUID.randomUUID();
      } while (SlotHash.getSlot(RedisMessageAvailabilityManager.getClientEventChannel(candidateIdentifier, secondDeviceId)) == firstSlot);

      secondAccountIdentifier = candidateIdentifier;
    }

    eventManager.handleClientConnected(firstAccountIdentifier, firstDeviceId, new MessageAvailabilityAdapter()).toCompletableFuture().join();
    eventManager.handleClientConnected(secondAccountIdentifier, secondDeviceId, new MessageAvailabilityAdapter()).toCompletableFuture().join();

    final int secondSlot = SlotHash.getSlot(RedisMessageAvailabilityManager.getClientEventChannel(secondAccountIdentifier, secondDeviceId));

    final String firstNodeId = UUID.randomUUID().toString();

    final RedisClusterNode firstBeforeNode = mock(RedisClusterNode.class);
    when(firstBeforeNode.getNodeId()).thenReturn(firstNodeId);
    when(firstBeforeNode.getSlots()).thenReturn(IntStream.range(0, SlotHash.SLOT_COUNT).boxed().toList());

    final RedisClusterNode firstAfterNode = mock(RedisClusterNode.class);
    when(firstAfterNode.getNodeId()).thenReturn(firstNodeId);
    when(firstAfterNode.getSlots()).thenReturn(IntStream.range(0, SlotHash.SLOT_COUNT)
        .filter(slot -> slot != secondSlot)
        .boxed()
        .toList());

    final RedisClusterNode secondAfterNode = mock(RedisClusterNode.class);
    when(secondAfterNode.getNodeId()).thenReturn(UUID.randomUUID().toString());
    when(secondAfterNode.getSlots()).thenReturn(List.of(secondSlot));

    eventManager.resubscribe(new ClusterTopologyChangedEvent(
        List.of(firstBeforeNode),
        List.of(firstAfterNode, secondAfterNode)));

    verify(pubSubCommands).ssubscribe(RedisMessageAvailabilityManager.getClientEventChannel(secondAccountIdentifier, secondDeviceId));
    verify(pubSubCommands, never()).ssubscribe(RedisMessageAvailabilityManager.getClientEventChannel(firstAccountIdentifier, firstDeviceId));
  }

  @Test
  void unsubscribeIfMissingListener() {
    @SuppressWarnings("unchecked") final RedisClusterPubSubAsyncCommands<byte[], byte[]> pubSubAsyncCommands =
        mock(RedisClusterPubSubAsyncCommands.class);

    when(pubSubAsyncCommands.ssubscribe(any())).thenReturn(MockRedisFuture.completedFuture(null));

    final FaultTolerantRedisClusterClient clusterClient = RedisClusterHelper.builder()
        .binaryPubSubAsyncCommands(pubSubAsyncCommands)
        .build();

    final RedisMessageAvailabilityManager eventManager = new RedisMessageAvailabilityManager(
        clusterClient,
        Runnable::run,
        Runnable::run);

    eventManager.start();

    final UUID listenerAccountIdentifier = UUID.randomUUID();
    final byte listenerDeviceId = Device.PRIMARY_ID;

    final UUID noListenerAccountIdentifier = UUID.randomUUID();
    final byte noListenerDeviceId = listenerDeviceId + 1;

    eventManager.handleClientConnected(listenerAccountIdentifier, listenerDeviceId, new MessageAvailabilityAdapter())
        .toCompletableFuture()
        .join();

    eventManager.unsubscribeIfMissingListener(
        new RedisMessageAvailabilityManager.AccountAndDeviceIdentifier(listenerAccountIdentifier, listenerDeviceId));

    eventManager.unsubscribeIfMissingListener(
        new RedisMessageAvailabilityManager.AccountAndDeviceIdentifier(noListenerAccountIdentifier, noListenerDeviceId));

    verify(pubSubAsyncCommands, never())
        .sunsubscribe(RedisMessageAvailabilityManager.getClientEventChannel(listenerAccountIdentifier, listenerDeviceId));

    verify(pubSubAsyncCommands)
        .sunsubscribe(RedisMessageAvailabilityManager.getClientEventChannel(noListenerAccountIdentifier, noListenerDeviceId));
  }
}
