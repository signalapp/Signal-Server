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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantPubSubClusterConnection;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.RedisClusterUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.Util;

/**
 * The pub/sub-based client presence manager uses the Redis 7 sharded pub/sub system to notify connected clients that
 * new messages are available for retrieval and report to senders whether a client was present to receive a message when
 * sent. This system makes a best effort to ensure that a given client has only a single open connection across the
 * fleet of servers, but cannot guarantee at-most-one behavior.
 */
public class PubSubClientEventManager extends RedisClusterPubSubAdapter<byte[], byte[]> implements Managed {

  private final FaultTolerantRedisClusterClient clusterClient;
  private final Executor listenerEventExecutor;

  private final UUID serverId = UUID.randomUUID();

  private final byte[] CLIENT_CONNECTED_EVENT_BYTES = ClientEvent.newBuilder()
      .setClientConnected(ClientConnectedEvent.newBuilder()
          .setServerId(UUIDUtil.toByteString(serverId))
          .build())
      .build()
      .toByteArray();

  @Nullable
  private FaultTolerantPubSubClusterConnection<byte[], byte[]> pubSubConnection;

  private final Map<AccountAndDeviceIdentifier, ClientEventListener> listenersByAccountAndDeviceIdentifier;

  private static final byte[] DISCONNECT_REQUESTED_EVENT_BYTES = ClientEvent.newBuilder()
      .setDisconnectRequested(DisconnectRequested.getDefaultInstance())
      .build()
      .toByteArray();

  private static final Counter PUBLISH_CLIENT_CONNECTION_EVENT_ERROR_COUNTER =
      Metrics.counter(MetricsUtil.name(PubSubClientEventManager.class, "publishClientConnectionEventError"));

  private static final Counter UNSUBSCRIBE_ERROR_COUNTER =
      Metrics.counter(MetricsUtil.name(PubSubClientEventManager.class, "unsubscribeError"));

  private static final Counter MESSAGE_WITHOUT_LISTENER_COUNTER =
      Metrics.counter(MetricsUtil.name(PubSubClientEventManager.class, "messageWithoutListener"));

  private static final String LISTENER_GAUGE_NAME =
      MetricsUtil.name(PubSubClientEventManager.class, "listeners");

  private static final Logger logger = LoggerFactory.getLogger(PubSubClientEventManager.class);

  @VisibleForTesting
  record AccountAndDeviceIdentifier(UUID accountIdentifier, byte deviceId) {
  }

  public PubSubClientEventManager(final FaultTolerantRedisClusterClient clusterClient,
                                  final Executor listenerEventExecutor) {

    this.clusterClient = clusterClient;
    this.listenerEventExecutor = listenerEventExecutor;

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
   * Marks the given device as "present" and registers a listener for new messages and conflicting connections. If the
   * given device already has a presence registered with this presence manager instance, that presence is displaced
   * immediately and the listener's {@link ClientEventListener#handleConnectionDisplaced(boolean)} method is called.
   *
   * @param accountIdentifier the account identifier for the newly-connected device
   * @param deviceId the ID of the newly-connected device within the given account
   * @param listener the listener to notify when new messages or conflicting connections arrive for the newly-conencted
   *                 device
   *
   * @return a future that yields a connection identifier when the new device's presence has been registered; the future
   * may fail if a pub/sub subscription could not be established, in which case callers should close the client's
   * connection to the server
   */
  public CompletionStage<UUID> handleClientConnected(final UUID accountIdentifier, final byte deviceId, final ClientEventListener listener) {
    if (pubSubConnection == null) {
      throw new IllegalStateException("Presence manager not started");
    }

    final UUID connectionId = UUID.randomUUID();
    final byte[] clientPresenceKey = getClientEventChannel(accountIdentifier, deviceId);
    final AtomicReference<ClientEventListener> displacedListener = new AtomicReference<>();
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
          subscribeFuture.set(pubSubConnection.withPubSubConnection(connection ->
              connection.async().ssubscribe(clientPresenceKey)));

          if (existingListener != null) {
            displacedListener.set(existingListener);
          }

          return listener;
        });

    if (displacedListener.get() != null) {
      listenerEventExecutor.execute(() -> displacedListener.get().handleConnectionDisplaced(true));
    }

    return subscribeFuture.get()
        .thenCompose(ignored -> clusterClient.withBinaryCluster(connection -> connection.async()
            .spublish(clientPresenceKey, CLIENT_CONNECTED_EVENT_BYTES)))
        .handle((ignored, throwable) -> {
          if (throwable != null) {
            PUBLISH_CLIENT_CONNECTION_EVENT_ERROR_COUNTER.increment();
          }

          return connectionId;
        });
  }

  /**
   * Removes the "presence" for the given device. Callers should call this method when they have been notified that
   * the client's underlying network connection has been closed.
   *
   * @param accountIdentifier the identifier of the account for the disconnected device
   * @param deviceId the ID of the disconnected device within the given account
   *
   * @return a future that completes when the presence has been removed
   */
  public CompletionStage<Void> handleClientDisconnected(final UUID accountIdentifier, final byte deviceId) {
    if (pubSubConnection == null) {
      throw new IllegalStateException("Presence manager not started");
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
          unsubscribeFuture.set(pubSubConnection.withPubSubConnection(connection ->
                  connection.async().sunsubscribe(getClientEventChannel(accountIdentifier, deviceId)))
              .thenRun(Util.NOOP));

          return null;
        });

    return unsubscribeFuture.get().whenComplete((ignored, throwable) -> {
      if (throwable != null) {
        UNSUBSCRIBE_ERROR_COUNTER.increment();
      }
    });
  }

  /**
   * Tests whether a client with the given account/device is connected to this presence manager instance.
   *
   * @param accountUuid the account identifier for the client to check
   * @param deviceId the ID of the device within the given account
   *
   * @return {@code true} if a client with the given account/device is connected to this presence manager instance or
   * {@code false} if the client is not connected at all or is connected to a different presence manager instance
   */
  public boolean isLocallyPresent(final UUID accountUuid, final byte deviceId) {
    return listenersByAccountAndDeviceIdentifier.containsKey(new AccountAndDeviceIdentifier(accountUuid, deviceId));
  }

  /**
   * Broadcasts a request that all devices associated with the identified account and connected to any client presence
   * instance close their network connections.
   *
   * @param accountIdentifier the account identifier for which to request disconnection
   *
   * @return a future that completes when the request has been sent
   */
  public CompletableFuture<Void> requestDisconnection(final UUID accountIdentifier) {
    return requestDisconnection(accountIdentifier, Device.ALL_POSSIBLE_DEVICE_IDS);
  }

  /**
   * Broadcasts a request that the specified devices associated with the identified account and connected to any client
   * presence instance close their network connections.
   *
   * @param accountIdentifier the account identifier for which to request disconnection
   * @param deviceIds the IDs of the devices for which to request disconnection
   *
   * @return a future that completes when the request has been sent
   */
  public CompletableFuture<Void> requestDisconnection(final UUID accountIdentifier, final Collection<Byte> deviceIds) {
    return CompletableFuture.allOf(deviceIds.stream()
        .map(deviceId -> {
          final byte[] clientPresenceKey = getClientEventChannel(accountIdentifier, deviceId);

          return clusterClient.withBinaryCluster(connection -> connection.async()
                  .spublish(clientPresenceKey, DISCONNECT_REQUESTED_EVENT_BYTES))
              .toCompletableFuture();
        })
        .toArray(CompletableFuture[]::new));
  }

  @VisibleForTesting
  void resubscribe(final ClusterTopologyChangedEvent clusterTopologyChangedEvent) {
    final boolean[] changedSlots = RedisClusterUtil.getChangedSlots(clusterTopologyChangedEvent);

    final Map<Integer, List<byte[]>> clientPresenceKeysBySlot = new HashMap<>();

    // Organize subscriptions by slot so we can issue a smaller number of larger resubscription commands
    listenersByAccountAndDeviceIdentifier.keySet()
            .stream()
            .map(accountAndDeviceIdentifier -> getClientEventChannel(accountAndDeviceIdentifier.accountIdentifier(), accountAndDeviceIdentifier.deviceId()))
            .forEach(clientEventChannel -> {
              final int slot = SlotHash.getSlot(clientEventChannel);

              if (changedSlots[slot]) {
                clientPresenceKeysBySlot.computeIfAbsent(slot, ignored -> new ArrayList<>()).add(clientEventChannel);
              }
            });

    // Issue one resubscription command per affected slot
    clientPresenceKeysBySlot.forEach((slot, clientPresenceKeys) -> {
      if (pubSubConnection != null) {
        final byte[][] clientPresenceKeyArray = clientPresenceKeys.toArray(byte[][]::new);
        pubSubConnection.usePubSubConnection(connection -> connection.sync().ssubscribe(clientPresenceKeyArray));
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
        pubSubConnection.usePubSubConnection(connection ->
            connection.async().sunsubscribe(getClientPresenceKey(accountAndDeviceIdentifier.accountIdentifier(),
                accountAndDeviceIdentifier.deviceId())));
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

    final AccountAndDeviceIdentifier accountAndDeviceIdentifier = parseClientPresenceKey(shardChannel);

    @Nullable final ClientEventListener listener =
        listenersByAccountAndDeviceIdentifier.get(accountAndDeviceIdentifier);

    if (listener != null) {
      switch (clientEvent.getEventCase()) {
        case NEW_MESSAGE_AVAILABLE -> listener.handleNewMessageAvailable();

        case CLIENT_CONNECTED -> {
          // Only act on new connections to other presence manager instances; we'll learn about displacements in THIS
          // instance when we update the listener map in `handleClientConnected`
          if (!this.serverId.equals(UUIDUtil.fromByteString(clientEvent.getClientConnected().getServerId()))) {
            listenerEventExecutor.execute(() -> listener.handleConnectionDisplaced(true));
          }
        }

        case DISCONNECT_REQUESTED -> listenerEventExecutor.execute(() -> listener.handleConnectionDisplaced(false));

        case MESSAGES_PERSISTED -> listenerEventExecutor.execute(listener::handleMessagesPersisted);

        default -> logger.warn("Unexpected client event type: {}", clientEvent.getClass());
      }
    } else {
      MESSAGE_WITHOUT_LISTENER_COUNTER.increment();
      listenerEventExecutor.execute(() -> unsubscribeIfMissingListener(accountAndDeviceIdentifier));
    }
  }

  public static byte[] getClientEventChannel(final UUID accountIdentifier, final byte deviceId) {
    return ("client_presence::{" + accountIdentifier + "::" + deviceId + "}").getBytes(StandardCharsets.UTF_8);
  }

  private static AccountAndDeviceIdentifier parseClientPresenceKey(final byte[] clientPresenceKeyBytes) {
    final String clientPresenceKey = new String(clientPresenceKeyBytes, StandardCharsets.UTF_8);
    final int uuidStart = "client_presence::{".length();

    final UUID accountIdentifier = UUID.fromString(clientPresenceKey.substring(uuidStart, uuidStart + 36));
    final byte deviceId = Byte.parseByte(clientPresenceKey.substring(uuidStart + 38, clientPresenceKey.length() - 1));

    return new AccountAndDeviceIdentifier(accountIdentifier, deviceId);
  }
}
