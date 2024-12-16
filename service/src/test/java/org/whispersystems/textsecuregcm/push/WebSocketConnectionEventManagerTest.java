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
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.api.async.RedisClusterPubSubAsyncCommands;
import io.lettuce.core.cluster.pubsub.api.sync.RedisClusterPubSubCommands;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.MockRedisFuture;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;

@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class WebSocketConnectionEventManagerTest {

  private WebSocketConnectionEventManager localEventManager;
  private WebSocketConnectionEventManager remoteEventManager;

  private static ExecutorService webSocketConnectionEventExecutor;
  private static ExecutorService asyncOperationQueueingExecutor;

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private static class WebSocketConnectionEventAdapter implements WebSocketConnectionEventListener {

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
    webSocketConnectionEventExecutor = Executors.newVirtualThreadPerTaskExecutor();
    asyncOperationQueueingExecutor = Executors.newSingleThreadExecutor();
  }

  @BeforeEach
  void setUp() {
    localEventManager = new WebSocketConnectionEventManager(mock(AccountsManager.class),
        mock(PushNotificationManager.class),
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        webSocketConnectionEventExecutor,
        asyncOperationQueueingExecutor);

    remoteEventManager = new WebSocketConnectionEventManager(mock(AccountsManager.class),
        mock(PushNotificationManager.class),
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
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
    final AtomicBoolean firstListenerConnectedElsewhere = new AtomicBoolean(false);

    localEventManager.handleClientConnected(accountIdentifier, deviceId, new WebSocketConnectionEventAdapter() {
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

    final WebSocketConnectionEventManager displacingManager =
        displaceRemotely ? remoteEventManager : localEventManager;

    displacingManager.handleClientConnected(accountIdentifier, deviceId, new WebSocketConnectionEventAdapter() {
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

    assertFalse(localEventManager.isLocallyPresent(accountIdentifier, deviceId));
    assertFalse(remoteEventManager.isLocallyPresent(accountIdentifier, deviceId));

    localEventManager.handleClientConnected(accountIdentifier, deviceId, new WebSocketConnectionEventAdapter())
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
  void handleDisconnectionRequest() throws InterruptedException {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte firstDeviceId = Device.PRIMARY_ID;
    final byte secondDeviceId = firstDeviceId + 1;

    final AtomicBoolean firstListenerDisplaced = new AtomicBoolean(false);
    final AtomicBoolean secondListenerDisplaced = new AtomicBoolean(false);

    final AtomicBoolean firstListenerConnectedElsewhere = new AtomicBoolean(false);

    localEventManager.handleClientConnected(accountIdentifier, firstDeviceId, new WebSocketConnectionEventAdapter() {
      @Override
      public void handleConnectionDisplaced(final boolean connectedElsewhere) {
        synchronized (firstListenerDisplaced) {
          firstListenerDisplaced.set(true);
          firstListenerConnectedElsewhere.set(connectedElsewhere);

          firstListenerDisplaced.notifyAll();
        }
      }
    }).toCompletableFuture().join();

    localEventManager.handleClientConnected(accountIdentifier, secondDeviceId, new WebSocketConnectionEventAdapter() {
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

    localEventManager.handleDisconnectionRequest(accountIdentifier, List.of(firstDeviceId));

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

    final WebSocketConnectionEventManager eventManager = new WebSocketConnectionEventManager(
        mock(AccountsManager.class),
        mock(PushNotificationManager.class),
        clusterClient,
        Runnable::run,
        Runnable::run);

    eventManager.start();

    final UUID firstAccountIdentifier = UUID.randomUUID();
    final byte firstDeviceId = Device.PRIMARY_ID;
    final int firstSlot = SlotHash.getSlot(WebSocketConnectionEventManager.getClientEventChannel(firstAccountIdentifier, firstDeviceId));

    final UUID secondAccountIdentifier;
    final byte secondDeviceId = firstDeviceId + 1;

    // Make sure that the two subscriptions wind up in different slots
    {
      UUID candidateIdentifier;

      do {
        candidateIdentifier = UUID.randomUUID();
      } while (SlotHash.getSlot(WebSocketConnectionEventManager.getClientEventChannel(candidateIdentifier, secondDeviceId)) == firstSlot);

      secondAccountIdentifier = candidateIdentifier;
    }

    eventManager.handleClientConnected(firstAccountIdentifier, firstDeviceId, new WebSocketConnectionEventAdapter()).toCompletableFuture().join();
    eventManager.handleClientConnected(secondAccountIdentifier, secondDeviceId, new WebSocketConnectionEventAdapter()).toCompletableFuture().join();

    final int secondSlot = SlotHash.getSlot(WebSocketConnectionEventManager.getClientEventChannel(secondAccountIdentifier, secondDeviceId));

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

    verify(pubSubCommands).ssubscribe(WebSocketConnectionEventManager.getClientEventChannel(secondAccountIdentifier, secondDeviceId));
    verify(pubSubCommands, never()).ssubscribe(WebSocketConnectionEventManager.getClientEventChannel(firstAccountIdentifier, firstDeviceId));
  }

  @Test
  void unsubscribeIfMissingListener() {
    @SuppressWarnings("unchecked") final RedisClusterPubSubAsyncCommands<byte[], byte[]> pubSubAsyncCommands =
        mock(RedisClusterPubSubAsyncCommands.class);

    when(pubSubAsyncCommands.ssubscribe(any())).thenReturn(MockRedisFuture.completedFuture(null));

    final FaultTolerantRedisClusterClient clusterClient = RedisClusterHelper.builder()
        .binaryPubSubAsyncCommands(pubSubAsyncCommands)
        .build();

    final WebSocketConnectionEventManager eventManager = new WebSocketConnectionEventManager(
        mock(AccountsManager.class),
        mock(PushNotificationManager.class),
        clusterClient,
        Runnable::run,
        Runnable::run);

    eventManager.start();

    final UUID listenerAccountIdentifier = UUID.randomUUID();
    final byte listenerDeviceId = Device.PRIMARY_ID;

    final UUID noListenerAccountIdentifier = UUID.randomUUID();
    final byte noListenerDeviceId = listenerDeviceId + 1;

    eventManager.handleClientConnected(listenerAccountIdentifier, listenerDeviceId, new WebSocketConnectionEventAdapter())
        .toCompletableFuture()
        .join();

    eventManager.unsubscribeIfMissingListener(
        new WebSocketConnectionEventManager.AccountAndDeviceIdentifier(listenerAccountIdentifier, listenerDeviceId));

    eventManager.unsubscribeIfMissingListener(
        new WebSocketConnectionEventManager.AccountAndDeviceIdentifier(noListenerAccountIdentifier, noListenerDeviceId));

    verify(pubSubAsyncCommands, never())
        .sunsubscribe(WebSocketConnectionEventManager.getClientEventChannel(listenerAccountIdentifier, listenerDeviceId));

    verify(pubSubAsyncCommands)
        .sunsubscribe(WebSocketConnectionEventManager.getClientEventChannel(noListenerAccountIdentifier, noListenerDeviceId));
  }

  @Test
  void newMessageNotificationWithoutListener() throws NotPushRegisteredException {
    final UUID listenerAccountIdentifier = UUID.randomUUID();
    final byte listenerDeviceId = Device.PRIMARY_ID;

    final UUID noListenerAccountIdentifier = UUID.randomUUID();
    final byte noListenerDeviceId = listenerDeviceId + 1;

    final Account noListenerAccount = mock(Account.class);

    final AccountsManager accountsManager = mock(AccountsManager.class);

    when(accountsManager.getByAccountIdentifierAsync(noListenerAccountIdentifier))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(noListenerAccount)));

    final PushNotificationManager pushNotificationManager = mock(PushNotificationManager.class);

    @SuppressWarnings("unchecked") final RedisClusterPubSubAsyncCommands<byte[], byte[]> pubSubAsyncCommands =
        mock(RedisClusterPubSubAsyncCommands.class);

    when(pubSubAsyncCommands.ssubscribe(any())).thenReturn(MockRedisFuture.completedFuture(null));

    final FaultTolerantRedisClusterClient clusterClient = RedisClusterHelper.builder()
        .binaryPubSubAsyncCommands(pubSubAsyncCommands)
        .build();

    final WebSocketConnectionEventManager eventManager = new WebSocketConnectionEventManager(
        accountsManager,
        pushNotificationManager,
        clusterClient,
        Runnable::run,
        Runnable::run);

    eventManager.start();

    eventManager.handleClientConnected(listenerAccountIdentifier, listenerDeviceId, new WebSocketConnectionEventAdapter())
        .toCompletableFuture()
        .join();

    final byte[] newMessagePayload = ClientEvent.newBuilder()
        .setNewMessageAvailable(NewMessageAvailableEvent.getDefaultInstance())
        .build()
        .toByteArray();

    eventManager.smessage(mock(RedisClusterNode.class),
        WebSocketConnectionEventManager.getClientEventChannel(listenerAccountIdentifier, listenerDeviceId),
        newMessagePayload);

    eventManager.smessage(mock(RedisClusterNode.class),
        WebSocketConnectionEventManager.getClientEventChannel(noListenerAccountIdentifier, noListenerDeviceId),
        newMessagePayload);

    verify(pushNotificationManager).sendNewMessageNotification(noListenerAccount, noListenerDeviceId, true);
    verifyNoMoreInteractions(pushNotificationManager);
  }
}
