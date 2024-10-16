/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantPubSubClusterConnection;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.storage.Device;

/**
 * The client presence manager keeps track of which clients are actively connected and "present" to receive messages.
 * Only one client per account/device may be present at a time; if a second client for the same account/device declares
 * its presence, the previous client is displaced.
 * <p/>
 * The client presence manager depends on Redis keyspace notifications and requires that the Redis instance support at
 * least the following notification types: {@code K$z}.
 */
public class ClientPresenceManager extends RedisClusterPubSubAdapter<String, String> implements Managed {

  private final String managerId = UUID.randomUUID().toString();
  private final String connectedClientSetKey = getConnectedClientSetKey(managerId);

  private final FaultTolerantRedisClusterClient presenceCluster;
  private final FaultTolerantPubSubClusterConnection<String, String> pubSubConnection;

  private final ClusterLuaScript clearPresenceScript;
  private final ClusterLuaScript renewPresenceScript;

  private final ExecutorService keyspaceNotificationExecutorService;
  private final ScheduledExecutorService scheduledExecutorService;
  private ScheduledFuture<?> pruneMissingPeersFuture;

  private final Map<String, DisplacedPresenceListener> displacementListenersByPresenceKey = new ConcurrentHashMap<>();
  private final Map<String, CompletionStage<?>> pendingPresenceSetsByPresenceKey = new ConcurrentHashMap<>();

  private final Timer checkPresenceTimer;
  private final Timer setPresenceTimer;
  private final Timer clearPresenceTimer;
  private final Timer prunePeersTimer;
  private final Counter pruneClientMeter;
  private final Counter remoteDisplacementMeter;
  private final Counter pubSubMessageMeter;
  private final Counter displacementListenerAlreadyRemovedCounter;

  private static final int PRUNE_PEERS_INTERVAL_SECONDS = (int) Duration.ofSeconds(30).toSeconds();
  private static final int PRESENCE_EXPIRATION_SECONDS = (int) Duration.ofMinutes(11).toSeconds();

  static final String MANAGER_SET_KEY = "presence::managers";

  private static final Logger log = LoggerFactory.getLogger(ClientPresenceManager.class);

  public ClientPresenceManager(final FaultTolerantRedisClusterClient presenceCluster,
      final ScheduledExecutorService scheduledExecutorService,
      final ExecutorService keyspaceNotificationExecutorService) throws IOException {
    this.presenceCluster = presenceCluster;
    this.pubSubConnection = this.presenceCluster.createPubSubConnection();
    this.clearPresenceScript = ClusterLuaScript.fromResource(presenceCluster, "lua/clear_presence.lua",
        ScriptOutputType.INTEGER);
    this.renewPresenceScript = ClusterLuaScript.fromResource(presenceCluster, "lua/renew_presence.lua",
        ScriptOutputType.VALUE);
    this.scheduledExecutorService = scheduledExecutorService;
    this.keyspaceNotificationExecutorService = keyspaceNotificationExecutorService;

    Metrics.gauge(name(getClass(), "localClientCount"), this, ignored -> displacementListenersByPresenceKey.size());

    this.checkPresenceTimer = Metrics.timer(name(getClass(), "checkPresence"));
    this.setPresenceTimer = Metrics.timer(name(getClass(), "setPresence"));
    this.clearPresenceTimer = Metrics.timer(name(getClass(), "clearPresence"));
    this.prunePeersTimer = Metrics.timer(name(getClass(), "prunePeers"));
    this.pruneClientMeter = Metrics.counter(name(getClass(), "pruneClient"));
    this.remoteDisplacementMeter = Metrics.counter(name(getClass(), "remoteDisplacement"));
    this.pubSubMessageMeter = Metrics.counter(name(getClass(), "pubSubMessage"));
    this.displacementListenerAlreadyRemovedCounter = Metrics.counter(
        name(getClass(), "displacementListenerAlreadyRemoved"));
  }

  @VisibleForTesting
  FaultTolerantPubSubClusterConnection<String, String> getPubSubConnection() {
    return pubSubConnection;
  }

  @Override
  public void start() {
    pubSubConnection.usePubSubConnection(connection -> {
      connection.addListener(this);

      final String presenceChannel = getManagerPresenceChannel(managerId);
      final int slot = SlotHash.getSlot(presenceChannel);

      connection.sync().nodes(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM) && node.hasSlot(slot))
          .commands()
          .subscribe(presenceChannel);
    });

    pubSubConnection.subscribeToClusterTopologyChangedEvents(this::resubscribeAll);

    presenceCluster.useCluster(connection -> connection.sync().sadd(MANAGER_SET_KEY, managerId));

    pruneMissingPeersFuture = scheduledExecutorService.scheduleWithFixedDelay(() -> {
      try {
        pruneMissingPeers();
      } catch (final Throwable t) {
        log.warn("Failed to prune missing peers", t);
      }
    }, new Random().nextInt(PRUNE_PEERS_INTERVAL_SECONDS), PRUNE_PEERS_INTERVAL_SECONDS, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    pubSubConnection.usePubSubConnection(connection -> connection.removeListener(this));

    if (pruneMissingPeersFuture != null) {
      pruneMissingPeersFuture.cancel(false);
    }

    for (final String presenceKey : displacementListenersByPresenceKey.keySet()) {
      clearPresence(presenceKey);
    }

    presenceCluster.useCluster(connection -> {
      connection.sync().srem(MANAGER_SET_KEY, managerId);
      connection.sync().del(getConnectedClientSetKey(managerId));
    });

    pubSubConnection.usePubSubConnection(
        connection -> connection.sync().upstream().commands().unsubscribe(getManagerPresenceChannel(managerId)));
  }

  public void setPresent(final UUID accountUuid, final byte deviceId,
      final DisplacedPresenceListener displacementListener) {

    setPresenceTimer.record(() -> {
      final String presenceKey = getPresenceKey(accountUuid, deviceId);

      displacePresence(presenceKey, true);

      displacementListenersByPresenceKey.put(presenceKey, displacementListener);

      final CompletableFuture<Void> presenceFuture = new CompletableFuture<>();
      final CompletionStage<?> previousFuture = pendingPresenceSetsByPresenceKey.put(presenceKey, presenceFuture);
      if (previousFuture != null) {
        log.debug("Another presence is already pending for {}:{}", accountUuid, deviceId);
      }

      subscribeForRemotePresenceChanges(presenceKey);

      presenceCluster.withCluster(connection -> {
        final RedisAdvancedClusterAsyncCommands<String, String> commands = connection.async();

        commands.sadd(connectedClientSetKey, presenceKey);
        return commands.setex(presenceKey, PRESENCE_EXPIRATION_SECONDS, managerId);
      }).whenComplete((result, throwable) -> {
        if (throwable != null) {
          presenceFuture.completeExceptionally(throwable);
        } else {
          presenceFuture.complete(null);
        }
      });

      presenceFuture.whenComplete(
          (ignored, throwable) -> pendingPresenceSetsByPresenceKey.remove(presenceKey, presenceFuture));
    });
  }

  public void renewPresence(final UUID accountUuid, final byte deviceId) {
    renewPresenceScript.execute(List.of(getPresenceKey(accountUuid, deviceId)),
        List.of(managerId, String.valueOf(PRESENCE_EXPIRATION_SECONDS)));
  }

  public void disconnectAllPresences(final UUID accountUuid, final List<Byte> deviceIds) {

    List<String> presenceKeys = new ArrayList<>();
    deviceIds.forEach(deviceId -> {
      String presenceKey = getPresenceKey(accountUuid, deviceId);
      if (isLocallyPresent(accountUuid, deviceId)) {
        displacePresence(presenceKey, false);
      }
      presenceKeys.add(presenceKey);
    });

    presenceCluster.useCluster(connection -> {
      List<RedisFuture<Long>> futures = presenceKeys.stream().map(key -> connection.async().del(key)).toList();
      LettuceFutures.awaitAll(connection.getTimeout(), futures.toArray(new RedisFuture[0]));
    });
  }

  public void disconnectAllPresencesForUuid(final UUID accountUuid) {
    disconnectAllPresences(accountUuid, Device.ALL_POSSIBLE_DEVICE_IDS);
  }

  public void disconnectPresence(final UUID accountUuid, final byte deviceId) {
    disconnectAllPresences(accountUuid, List.of(deviceId));
  }

  private void displacePresence(final String presenceKey, final boolean connectedElsewhere) {
    final DisplacedPresenceListener displacementListener = displacementListenersByPresenceKey.get(presenceKey);

    if (displacementListener != null) {
      displacementListener.handleDisplacement(connectedElsewhere);
    }

    clearPresence(presenceKey);
  }

  public boolean isPresent(final UUID accountUuid, final byte deviceId) {
    return checkPresenceTimer.record(() ->
        presenceCluster.withCluster(connection ->
            connection.sync().exists(getPresenceKey(accountUuid, deviceId))) == 1);
  }

  public boolean isLocallyPresent(final UUID accountUuid, final byte deviceId) {
    return displacementListenersByPresenceKey.containsKey(getPresenceKey(accountUuid, deviceId));
  }

  public boolean clearPresence(final UUID accountUuid, final byte deviceId, final DisplacedPresenceListener listener) {
    final String presenceKey = getPresenceKey(accountUuid, deviceId);
    if (displacementListenersByPresenceKey.remove(presenceKey, listener)) {
      return clearPresence(presenceKey);
    } else {
      displacementListenerAlreadyRemovedCounter.increment();
      return false;
    }
  }

  private boolean clearPresence(final String presenceKey) {
    return clearPresenceTimer.record(() -> {
      displacementListenersByPresenceKey.remove(presenceKey);
      unsubscribeFromRemotePresenceChanges(presenceKey);

      final boolean removed = clearPresenceScript.execute(List.of(presenceKey), List.of(managerId)) != null;
      presenceCluster.useCluster(connection -> connection.sync().srem(connectedClientSetKey, presenceKey));

      return removed;
    });
  }

  private void subscribeForRemotePresenceChanges(final String presenceKey) {
    final int slot = SlotHash.getSlot(presenceKey);

    pubSubConnection.usePubSubConnection(
        connection -> connection.sync().nodes(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM) && node.hasSlot(slot))
            .commands()
            .subscribe(getKeyspaceNotificationChannel(presenceKey)));
  }

  private void resubscribeAll() {
    for (final String presenceKey : displacementListenersByPresenceKey.keySet()) {
      subscribeForRemotePresenceChanges(presenceKey);
    }
  }

  private void unsubscribeFromRemotePresenceChanges(final String presenceKey) {
    pubSubConnection.usePubSubConnection(
        connection -> connection.sync().upstream().commands().unsubscribe(getKeyspaceNotificationChannel(presenceKey)));
  }

  void pruneMissingPeers() {
    prunePeersTimer.record(() -> {
      final Set<String> peerIds = presenceCluster.withCluster(
          connection -> connection.sync().smembers(MANAGER_SET_KEY));
      peerIds.remove(managerId);

      for (final String peerId : peerIds) {
        final boolean peerMissing = presenceCluster.withCluster(
            connection -> connection.sync().publish(getManagerPresenceChannel(peerId), "ping") == 0);

        if (peerMissing) {
          log.debug("Presence manager {} did not respond to ping", peerId);

          final String connectedClientsKey = getConnectedClientSetKey(peerId);

          String presenceKey;

          while ((presenceKey = presenceCluster.withCluster(connection -> connection.sync().spop(connectedClientsKey)))
              != null) {
            clearPresenceScript.execute(List.of(presenceKey), List.of(peerId));
            pruneClientMeter.increment();
          }

          presenceCluster.useCluster(connection -> {
            connection.sync().del(connectedClientsKey);
            connection.sync().srem(MANAGER_SET_KEY, peerId);
          });
        }
      }
    });
  }

  @Override
  public void message(final RedisClusterNode node, final String channel, final String message) {
    pubSubMessageMeter.increment();

    if (channel.startsWith("__keyspace@0__:presence::{")) {
      if ("set".equals(message) || "del".equals(message)) {
        // "set" might mean the client has connected to another host, although it might just be our own `set`,
        // because we subscribe for changes before setting the key.
        // for "del", another process has indicated the client should be disconnected
        final boolean maybeConnectedElsewhere = "set".equals(message);

        // At this point, we're on a Lettuce IO thread and need to dispatch to a separate thread before making
        // synchronous Lettuce calls to avoid deadlocking.
        keyspaceNotificationExecutorService.execute(() -> {
            final String clientPresenceKey = channel.substring("__keyspace@0__:".length());

          final CompletionStage<?> pendingConnection = pendingPresenceSetsByPresenceKey.getOrDefault(clientPresenceKey,
              CompletableFuture.completedFuture(null));

          pendingConnection.thenCompose(ignored -> {
                if (maybeConnectedElsewhere) {
                  return presenceCluster.withCluster(connection -> connection.async().get(clientPresenceKey))
                      .thenApply(currentManagerId -> !managerId.equals(currentManagerId));
                }

                return CompletableFuture.completedFuture(true);
              })
              .exceptionally(ignored -> true)
              .thenAcceptAsync(shouldDisplace -> {
                if (shouldDisplace) {
                  try {
                    displacePresence(clientPresenceKey, maybeConnectedElsewhere);
                    remoteDisplacementMeter.increment();
                  } catch (final Exception e) {
                    log.warn("Error displacing presence", e);
                  }
                }
              }, keyspaceNotificationExecutorService);
        });
      }
    }
  }

  @VisibleForTesting
  String getManagerId() {
    return managerId;
  }

  @VisibleForTesting
  static String getPresenceKey(final UUID accountUuid, final byte deviceId) {
    return "presence::{" + accountUuid.toString() + "::" + deviceId + "}";
  }

  private static String getKeyspaceNotificationChannel(final String presenceKey) {
    return "__keyspace@0__:" + presenceKey;
  }

  @VisibleForTesting
  static String getConnectedClientSetKey(final String managerId) {
    return "presence::clients::" + managerId;
  }

  @VisibleForTesting
  static String getManagerPresenceChannel(final String managerId) {
    return "presence::manager::" + managerId;
  }
}
