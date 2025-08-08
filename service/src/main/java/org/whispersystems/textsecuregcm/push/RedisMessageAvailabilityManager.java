/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantPubSubClusterConnection;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.RedisClusterUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.Util;

/**
 * The Redis message availability manager distributes events related to client presence and message availability to
 * registered listeners. In the current Signal server implementation, clients generally interact with the service by
 * opening a dual-purpose WebSocket. The WebSocket serves as both a delivery mechanism for messages and as a channel
 * for the client to issue API requests to the server. Clients are considered "present" if they have an open WebSocket
 * connection and are therefore likely to receive messages as soon as they're delivered to the server. Redis message
 * availability managers ensure that clients have at most one active message delivery channel at a time on a
 * best-effort basis.
 *
 * @implNote The Redis message availability manager uses the Redis 7 sharded pub/sub system to distribute events. This
 * system makes a best effort to ensure that a given client has only a single open connection across the fleet of
 * servers, but cannot guarantee at-most-one behavior.
 *
 * @see MessageAvailabilityListener
 * @see org.whispersystems.textsecuregcm.storage.MessagesManager#insert(UUID, Map)
 */
public class RedisMessageAvailabilityManager extends RedisClusterPubSubAdapter<byte[], byte[]> implements Managed {

  private final FaultTolerantRedisClusterClient clusterClient;
  private final Executor listenerEventExecutor;

  // Note that this MUST be a single-threaded executor; its function is to process tasks that should usually be
  // non-blocking, but can rarely block, and do so in the order in which those tasks were submitted.
  private final Executor asyncOperationQueueingExecutor;

  @Nullable
  private FaultTolerantPubSubClusterConnection<byte[], byte[]> pubSubConnection;

  private final Map<AccountAndDeviceIdentifier, MessageAvailabilityListener> listenersByAccountAndDeviceIdentifier;

  private final UUID serverId = UUID.randomUUID();

  private final byte[] CLIENT_CONNECTED_EVENT_BYTES = ClientEvent.newBuilder()
      .setClientConnected(ClientConnectedEvent.newBuilder()
          .setServerId(UUIDUtil.toByteString(serverId))
          .build())
      .build()
      .toByteArray();

  private static final Counter PUBLISH_CLIENT_CONNECTION_EVENT_ERROR_COUNTER =
      Metrics.counter(MetricsUtil.name(RedisMessageAvailabilityManager.class, "publishClientConnectionEventError"));

  private static final Counter UNSUBSCRIBE_ERROR_COUNTER =
      Metrics.counter(MetricsUtil.name(RedisMessageAvailabilityManager.class, "unsubscribeError"));

  private static final Counter PUB_SUB_EVENT_WITHOUT_LISTENER_COUNTER =
      Metrics.counter(MetricsUtil.name(RedisMessageAvailabilityManager.class, "pubSubEventWithoutListener"));

  private static final Counter MESSAGE_AVAILABLE_WITHOUT_LISTENER_COUNTER =
      Metrics.counter(MetricsUtil.name(RedisMessageAvailabilityManager.class, "messageAvailableWithoutListener"));

  private static final String LISTENER_GAUGE_NAME =
      MetricsUtil.name(RedisMessageAvailabilityManager.class, "listeners");

  private static final Logger logger = LoggerFactory.getLogger(RedisMessageAvailabilityManager.class);

  @VisibleForTesting
  record AccountAndDeviceIdentifier(UUID accountIdentifier, byte deviceId) {
  }

  public RedisMessageAvailabilityManager(final FaultTolerantRedisClusterClient clusterClient,
      final Executor listenerEventExecutor,
      final Executor asyncOperationQueueingExecutor) {

    this.clusterClient = clusterClient;
    this.listenerEventExecutor = listenerEventExecutor;
    this.asyncOperationQueueingExecutor = asyncOperationQueueingExecutor;

    this.listenersByAccountAndDeviceIdentifier =
        Metrics.gaugeMapSize(LISTENER_GAUGE_NAME, Tags.empty(), new ConcurrentHashMap<>());
  }

  @Override
  public synchronized void start() {
    this.pubSubConnection = clusterClient.createBinaryPubSubConnection();
    this.pubSubConnection.usePubSubConnection(connection -> connection.addListener(this));

    pubSubConnection.subscribeToClusterTopologyChangedEvents(this::resubscribe);
  }

  @Override
  public synchronized void stop() {
    if (pubSubConnection != null) {
      pubSubConnection.usePubSubConnection(connection -> {
        connection.removeListener(this);
        connection.close();
      });
    }

    pubSubConnection = null;
  }

  /**
   * Marks the given device as "present" for message delivery and registers a listener for new messages and conflicting
   * connections. If the given device already has a presence registered with this manager, that presence is displaced
   * immediately and the listener's {@link MessageAvailabilityListener#handleConflictingMessageConsumer()} method is called.
   *
   * @param accountIdentifier the account identifier for the newly-connected device
   * @param deviceId the ID of the newly-connected device within the given account
   * @param listener the listener to notify when new messages or conflicting connections arrive for the newly-connected
   *                 device
   *
   * @return a future that completes when the new device's presence has been registered; the future may fail if a
   * pub/sub subscription could not be established, in which case callers should close the client's connection to the
   * server
   */
  public CompletionStage<Void> handleClientConnected(final UUID accountIdentifier, final byte deviceId, final MessageAvailabilityListener listener) {
    if (pubSubConnection == null) {
      throw new IllegalStateException("WebSocket connection event manager not started");
    }

    final byte[] eventChannel = getClientEventChannel(accountIdentifier, deviceId);
    final AtomicReference<MessageAvailabilityListener> displacedListener = new AtomicReference<>();
    final AtomicReference<CompletionStage<Void>> subscribeFuture = new AtomicReference<>();

    // Note that we're relying on some specific implementation details of `ConcurrentHashMap#compute(...)`. In
    // particular, the behavioral contract for `ConcurrentHashMap#compute(...)` says:
    //
    // > The entire method invocation is performed atomically. The supplied function is invoked exactly once per
    // > invocation of this method. Some attempted update operations on this map by other threads may be blocked while
    // > computation is in progress, so the computation should be short and simple.
    //
    // This provides a mechanism to make sure that we enqueue subscription/unsubscription operations in the same order
    // as adding/removing listeners from the map and helps us avoid races and conflicts. Note that the enqueued
    // operation is asynchronous; we're not blocking on it in the scope of the `compute` operation.
    listenersByAccountAndDeviceIdentifier.compute(new AccountAndDeviceIdentifier(accountIdentifier, deviceId),
        (key, existingListener) -> {
          subscribeFuture.set(CompletableFuture.supplyAsync(() -> pubSubConnection.withPubSubConnection(connection ->
                  connection.async().ssubscribe(eventChannel)), asyncOperationQueueingExecutor)
              .thenCompose(Function.identity()));

          if (existingListener != null) {
            displacedListener.set(existingListener);
          }

          return listener;
        });

    if (displacedListener.get() != null) {
      listenerEventExecutor.execute(() -> displacedListener.get().handleConflictingMessageConsumer());
    }

    return subscribeFuture.get()
        .thenCompose(ignored -> clusterClient.withBinaryCluster(connection -> connection.async()
            .spublish(eventChannel, CLIENT_CONNECTED_EVENT_BYTES)))
        .handle((ignored, throwable) -> {
          if (throwable != null) {
            PUBLISH_CLIENT_CONNECTION_EVENT_ERROR_COUNTER.increment();
          }

          return null;
        });
  }

  /**
   * Removes the "presence" and event listener for the given device. Callers should call this method when the client's
   * underlying network connection has closed.
   *
   * @param accountIdentifier the identifier of the account for the disconnected device
   * @param deviceId the ID of the disconnected device within the given account
   *
   * @return a future that completes when the presence and event listener have been removed
   */
  public CompletionStage<Void> handleClientDisconnected(final UUID accountIdentifier, final byte deviceId) {
    if (pubSubConnection == null) {
      throw new IllegalStateException("WebSocket connection event manager not started");
    }

    final AtomicReference<CompletionStage<Void>> unsubscribeFuture = new AtomicReference<>();

    // Note that we're relying on some specific implementation details of `ConcurrentHashMap#compute(...)`. In
    // particular, the behavioral contract for `ConcurrentHashMap#compute(...)` says:
    //
    // > The entire method invocation is performed atomically. The supplied function is invoked exactly once per
    // > invocation of this method. Some attempted update operations on this map by other threads may be blocked while
    // > computation is in progress, so the computation should be short and simple.
    //
    // This provides a mechanism to make sure that we enqueue subscription/unsubscription operations in the same order
    // as adding/removing listeners from the map and helps us avoid races and conflicts. Note that the enqueued
    // operation is asynchronous; we're not blocking on it in the scope of the `compute` operation.
    listenersByAccountAndDeviceIdentifier.compute(new AccountAndDeviceIdentifier(accountIdentifier, deviceId),
        (ignored, existingListener) -> {
          unsubscribeFuture.set(CompletableFuture.supplyAsync(() -> pubSubConnection.withPubSubConnection(connection ->
                      connection.async().sunsubscribe(getClientEventChannel(accountIdentifier, deviceId)))
                  .thenRun(Util.NOOP), asyncOperationQueueingExecutor)
              .thenCompose(Function.identity()));

          return null;
        });

    return unsubscribeFuture.get().whenComplete((ignored, throwable) -> {
      if (throwable != null) {
        UNSUBSCRIBE_ERROR_COUNTER.increment();
      }
    });
  }

  /**
   * Tests whether a client with the given account/device is connected to this manager instance.
   *
   * @param accountUuid the account identifier for the client to check
   * @param deviceId the ID of the device within the given account
   *
   * @return {@code true} if a client with the given account/device is connected to this manager instance or
   * {@code false} if the client is not connected at all or is connected to a different manager instance
   */
  public boolean isLocallyPresent(final UUID accountUuid, final byte deviceId) {
    return listenersByAccountAndDeviceIdentifier.containsKey(new AccountAndDeviceIdentifier(accountUuid, deviceId));
  }

  @VisibleForTesting
  void resubscribe(final ClusterTopologyChangedEvent clusterTopologyChangedEvent) {
    final boolean[] changedSlots = RedisClusterUtil.getChangedSlots(clusterTopologyChangedEvent);

    final Map<Integer, List<byte[]>> eventChannelsBySlot = new HashMap<>();

    // Organize subscriptions by slot so we can issue a smaller number of larger resubscription commands
    listenersByAccountAndDeviceIdentifier.keySet()
            .stream()
            .map(accountAndDeviceIdentifier -> getClientEventChannel(accountAndDeviceIdentifier.accountIdentifier(), accountAndDeviceIdentifier.deviceId()))
            .forEach(clientEventChannel -> {
              final int slot = SlotHash.getSlot(clientEventChannel);

              if (changedSlots[slot]) {
                eventChannelsBySlot.computeIfAbsent(slot, ignored -> new ArrayList<>()).add(clientEventChannel);
              }
            });

    // Issue one resubscription command per affected slot
    eventChannelsBySlot.forEach((slot, eventChannels) -> {
      if (pubSubConnection != null) {
        pubSubConnection.usePubSubConnection(connection ->
            connection.sync().ssubscribe(eventChannels.toArray(byte[][]::new)));
      }
    });
  }

  /**
   * Unsubscribes for notifications for the given account and device identifier if and only if no listener is registered
   * for that account and device identifier.
   *
   * @param accountAndDeviceIdentifier the account and device identifier for which to stop receiving notifications
   */
  void unsubscribeIfMissingListener(final AccountAndDeviceIdentifier accountAndDeviceIdentifier) {
    listenersByAccountAndDeviceIdentifier.compute(accountAndDeviceIdentifier, (ignored, existingListener) -> {
      if (existingListener == null && pubSubConnection != null) {
        // Enqueue, but do not block on, an "unsubscribe" operation
        asyncOperationQueueingExecutor.execute(() -> pubSubConnection.usePubSubConnection(connection ->
            connection.async().sunsubscribe(getClientEventChannel(accountAndDeviceIdentifier.accountIdentifier(),
                accountAndDeviceIdentifier.deviceId()))));
      }

      // Make no change to the existing listener whether present or absent
      return existingListener;
    });
  }

  @Override
  public void smessage(final RedisClusterNode node, final byte[] shardChannel, final byte[] message) {
    final ClientEvent clientEvent;

    try {
      clientEvent = ClientEvent.parseFrom(message);
    } catch (final InvalidProtocolBufferException e) {
      logger.error("Failed to parse pub/sub event protobuf", e);
      return;
    }

    final AccountAndDeviceIdentifier accountAndDeviceIdentifier = parseClientEventChannel(shardChannel);

    @Nullable final MessageAvailabilityListener listener =
        listenersByAccountAndDeviceIdentifier.get(accountAndDeviceIdentifier);

    if (listener != null) {
      switch (clientEvent.getEventCase()) {
        case NEW_MESSAGE_AVAILABLE -> listener.handleNewMessageAvailable();

        case CLIENT_CONNECTED -> {
          // Only act on new connections to other event manager instances; we'll learn about displacements in THIS
          // instance when we update the listener map in `handleClientConnected`
          if (!this.serverId.equals(UUIDUtil.fromByteString(clientEvent.getClientConnected().getServerId()))) {
            listenerEventExecutor.execute(listener::handleConflictingMessageConsumer);
          }
        }

        case MESSAGES_PERSISTED -> listenerEventExecutor.execute(listener::handleMessagesPersisted);

        default -> logger.warn("Unexpected client event type: {}", clientEvent.getClass());
      }
    } else {
      PUB_SUB_EVENT_WITHOUT_LISTENER_COUNTER.increment();

      listenerEventExecutor.execute(() -> unsubscribeIfMissingListener(accountAndDeviceIdentifier));

      if (clientEvent.getEventCase() == ClientEvent.EventCase.NEW_MESSAGE_AVAILABLE) {
        MESSAGE_AVAILABLE_WITHOUT_LISTENER_COUNTER.increment();
      }
    }
  }

  public static byte[] getClientEventChannel(final UUID accountIdentifier, final byte deviceId) {
    return ("client_presence::{" + accountIdentifier + "::" + deviceId + "}").getBytes(StandardCharsets.UTF_8);
  }

  private static AccountAndDeviceIdentifier parseClientEventChannel(final byte[] eventChannelBytes) {
    final String eventChannel = new String(eventChannelBytes, StandardCharsets.UTF_8);
    final int uuidStart = "client_presence::{".length();

    final UUID accountIdentifier = UUID.fromString(eventChannel.substring(uuidStart, uuidStart + 36));
    final byte deviceId = Byte.parseByte(eventChannel.substring(uuidStart + 38, eventChannel.length() - 1));

    return new AccountAndDeviceIdentifier(accountIdentifier, deviceId);
  }
}
