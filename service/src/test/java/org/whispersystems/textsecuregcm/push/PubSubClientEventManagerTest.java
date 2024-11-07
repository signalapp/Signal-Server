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
class PubSubClientEventManagerTest {

  private PubSubClientEventManager localPresenceManager;
  private PubSubClientEventManager remotePresenceManager;

  private static ExecutorService clientEventExecutor;

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private static class ClientEventAdapter implements ClientEventListener {

    @Override
    public void handleNewMessageAvailable() {
    }

    @Override
    public void handleMessagesPersisted() {
    }

    @Override
    public void handleConnectionDisplaced(final boolean connectedElsewhere) {
    }
  }

  @BeforeAll
  static void setUpBeforeAll() {
    clientEventExecutor = Executors.newVirtualThreadPerTaskExecutor();
  }

  @BeforeEach
  void setUp() {
    localPresenceManager = new PubSubClientEventManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(), clientEventExecutor);
    remotePresenceManager = new PubSubClientEventManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(), clientEventExecutor);

    localPresenceManager.start();
    remotePresenceManager.start();
  }

  @AfterEach
  void tearDown() {
    localPresenceManager.stop();
    remotePresenceManager.stop();
  }

  @AfterAll
  static void tearDownAfterAll() {
    clientEventExecutor.shutdown();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void handleClientConnected(final boolean displaceRemotely) throws InterruptedException {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final AtomicBoolean firstListenerDisplaced = new AtomicBoolean(false);

    final AtomicBoolean secondListenerDisplaced = new AtomicBoolean(false);
    final AtomicBoolean firstListenerConnectedElsewhere = new AtomicBoolean(false);

    localPresenceManager.handleClientConnected(accountIdentifier, deviceId, new ClientEventAdapter() {
      @Override
      public void handleConnectionDisplaced(final boolean connectedElsewhere) {
        synchronized (firstListenerDisplaced) {
          firstListenerDisplaced.set(true);
          firstListenerConnectedElsewhere.set(connectedElsewhere);

          firstListenerDisplaced.notifyAll();
        }
      }
    }).toCompletableFuture().join();

    assertFalse(firstListenerDisplaced.get());
    assertFalse(secondListenerDisplaced.get());

    final PubSubClientEventManager displacingManager =
        displaceRemotely ? remotePresenceManager : localPresenceManager;

    displacingManager.handleClientConnected(accountIdentifier, deviceId, new ClientEventAdapter() {
      @Override
      public void handleConnectionDisplaced(final boolean connectedElsewhere) {
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

    assertTrue(firstListenerConnectedElsewhere.get());
  }

  @Test
  void isLocallyPresent() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    assertFalse(localPresenceManager.isLocallyPresent(accountIdentifier, deviceId));
    assertFalse(remotePresenceManager.isLocallyPresent(accountIdentifier, deviceId));

    localPresenceManager.handleClientConnected(accountIdentifier, deviceId, new ClientEventAdapter())
        .toCompletableFuture()
        .join();

    assertTrue(localPresenceManager.isLocallyPresent(accountIdentifier, deviceId));
    assertFalse(remotePresenceManager.isLocallyPresent(accountIdentifier, deviceId));

    localPresenceManager.handleClientDisconnected(accountIdentifier, deviceId)
        .toCompletableFuture()
        .join();

    assertFalse(localPresenceManager.isLocallyPresent(accountIdentifier, deviceId));
    assertFalse(remotePresenceManager.isLocallyPresent(accountIdentifier, deviceId));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void requestDisconnection(final boolean requestDisconnectionRemotely) throws InterruptedException {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte firstDeviceId = Device.PRIMARY_ID;
    final byte secondDeviceId = firstDeviceId + 1;

    final AtomicBoolean firstListenerDisplaced = new AtomicBoolean(false);
    final AtomicBoolean secondListenerDisplaced = new AtomicBoolean(false);

    final AtomicBoolean firstListenerConnectedElsewhere = new AtomicBoolean(false);

    localPresenceManager.handleClientConnected(accountIdentifier, firstDeviceId, new ClientEventAdapter() {
      @Override
      public void handleConnectionDisplaced(final boolean connectedElsewhere) {
        synchronized (firstListenerDisplaced) {
          firstListenerDisplaced.set(true);
          firstListenerConnectedElsewhere.set(connectedElsewhere);

          firstListenerDisplaced.notifyAll();
        }
      }
    }).toCompletableFuture().join();

    localPresenceManager.handleClientConnected(accountIdentifier, secondDeviceId, new ClientEventAdapter() {
      @Override
      public void handleConnectionDisplaced(final boolean connectedElsewhere) {
        synchronized (secondListenerDisplaced) {
          secondListenerDisplaced.set(true);
          secondListenerDisplaced.notifyAll();
        }
      }
    }).toCompletableFuture().join();

    assertFalse(firstListenerDisplaced.get());
    assertFalse(secondListenerDisplaced.get());

    final PubSubClientEventManager displacingManager =
        requestDisconnectionRemotely ? remotePresenceManager : localPresenceManager;

    displacingManager.requestDisconnection(accountIdentifier, List.of(firstDeviceId)).toCompletableFuture().join();

    synchronized (firstListenerDisplaced) {
      while (!firstListenerDisplaced.get()) {
        firstListenerDisplaced.wait();
      }
    }

    assertTrue(firstListenerDisplaced.get());
    assertFalse(secondListenerDisplaced.get());

    assertFalse(firstListenerConnectedElsewhere.get());
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

    final PubSubClientEventManager presenceManager = new PubSubClientEventManager(clusterClient, Runnable::run);

    presenceManager.start();

    final UUID firstAccountIdentifier = UUID.randomUUID();
    final byte firstDeviceId = Device.PRIMARY_ID;
    final int firstSlot = SlotHash.getSlot(PubSubClientEventManager.getClientEventChannel(firstAccountIdentifier, firstDeviceId));

    final UUID secondAccountIdentifier;
    final byte secondDeviceId = firstDeviceId + 1;

    // Make sure that the two subscriptions wind up in different slots
    {
      UUID candidateIdentifier;

      do {
        candidateIdentifier = UUID.randomUUID();
      } while (SlotHash.getSlot(PubSubClientEventManager.getClientEventChannel(candidateIdentifier, secondDeviceId)) == firstSlot);

      secondAccountIdentifier = candidateIdentifier;
    }

    presenceManager.handleClientConnected(firstAccountIdentifier, firstDeviceId, new ClientEventAdapter()).toCompletableFuture().join();
    presenceManager.handleClientConnected(secondAccountIdentifier, secondDeviceId, new ClientEventAdapter()).toCompletableFuture().join();

    final int secondSlot = SlotHash.getSlot(PubSubClientEventManager.getClientEventChannel(secondAccountIdentifier, secondDeviceId));

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

    presenceManager.resubscribe(new ClusterTopologyChangedEvent(
        List.of(firstBeforeNode),
        List.of(firstAfterNode, secondAfterNode)));

    verify(pubSubCommands).ssubscribe(PubSubClientEventManager.getClientEventChannel(secondAccountIdentifier, secondDeviceId));
    verify(pubSubCommands, never()).ssubscribe(PubSubClientEventManager.getClientEventChannel(firstAccountIdentifier, firstDeviceId));
  }
}
