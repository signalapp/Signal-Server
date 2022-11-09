/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantPubSubConnection;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.Constants;

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

  private final FaultTolerantRedisCluster presenceCluster;
  private final FaultTolerantPubSubConnection<String, String> pubSubConnection;

  private final ClusterLuaScript clearPresenceScript;
  private final ClusterLuaScript renewPresenceScript;

  private final ExecutorService keyspaceNotificationExecutorService;
  private final ScheduledExecutorService scheduledExecutorService;
  private ScheduledFuture<?> pruneMissingPeersFuture;

  private final Map<String, DisplacedPresenceListener> displacementListenersByPresenceKey = new ConcurrentHashMap<>();

  private final Timer checkPresenceTimer;
  private final Timer setPresenceTimer;
  private final Timer clearPresenceTimer;
  private final Timer prunePeersTimer;
  private final Meter pruneClientMeter;
  private final Meter remoteDisplacementMeter;
  private final Meter pubSubMessageMeter;

  private static final int PRUNE_PEERS_INTERVAL_SECONDS = (int) Duration.ofSeconds(30).toSeconds();
  private static final int PRESENCE_EXPIRATION_SECONDS = (int) Duration.ofMinutes(11).toSeconds();

  static final String MANAGER_SET_KEY = "presence::managers";

  private static final Logger log = LoggerFactory.getLogger(ClientPresenceManager.class);

  public ClientPresenceManager(final FaultTolerantRedisCluster presenceCluster,
      final ScheduledExecutorService scheduledExecutorService,
      final ExecutorService keyspaceNotificationExecutorService) throws IOException {
    this.presenceCluster = presenceCluster;
    this.pubSubConnection = this.presenceCluster.createPubSubConnection();
    this.clearPresenceScript = ClusterLuaScript.fromResource(presenceCluster, "lua/clear_presence.lua", ScriptOutputType.INTEGER);
    this.renewPresenceScript = ClusterLuaScript.fromResource(presenceCluster, "lua/renew_presence.lua", ScriptOutputType.VALUE);
    this.scheduledExecutorService = scheduledExecutorService;
    this.keyspaceNotificationExecutorService = keyspaceNotificationExecutorService;

    final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
    metricRegistry.gauge(name(getClass(), "localClientCount"), () -> displacementListenersByPresenceKey::size);

    this.checkPresenceTimer = metricRegistry.timer(name(getClass(), "checkPresence"));
    this.setPresenceTimer = metricRegistry.timer(name(getClass(), "setPresence"));
    this.clearPresenceTimer = metricRegistry.timer(name(getClass(), "clearPresence"));
    this.prunePeersTimer = metricRegistry.timer(name(getClass(), "prunePeers"));
    this.pruneClientMeter = metricRegistry.meter(name(getClass(), "pruneClient"));
    this.remoteDisplacementMeter = metricRegistry.meter(name(getClass(), "remoteDisplacement"));
    this.pubSubMessageMeter = metricRegistry.meter(name(getClass(), "pubSubMessage"));
  }

  @VisibleForTesting
  FaultTolerantPubSubConnection<String, String> getPubSubConnection() {
    return pubSubConnection;
  }

  @Override
  public void start() {
    pubSubConnection.usePubSubConnection(connection -> {
      connection.addListener(this);
      connection.getResources().eventBus().get()
          .filter(event -> event instanceof ClusterTopologyChangedEvent)
          .subscribe(event -> resubscribeAll());

      final String presenceChannel = getManagerPresenceChannel(managerId);
      final int slot = SlotHash.getSlot(presenceChannel);

      connection.sync().nodes(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM) && node.hasSlot(slot))
          .commands()
          .subscribe(presenceChannel);
    });

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

  public void setPresent(final UUID accountUuid, final long deviceId, final DisplacedPresenceListener displacementListener) {

    try (final Timer.Context ignored = setPresenceTimer.time()) {
      final String presenceKey = getPresenceKey(accountUuid, deviceId);

      displacePresence(presenceKey, true);

      displacementListenersByPresenceKey.put(presenceKey, displacementListener);

      presenceCluster.useCluster(connection -> {
        final RedisAdvancedClusterCommands<String, String> commands = connection.sync();

        commands.sadd(connectedClientSetKey, presenceKey);
        commands.setex(presenceKey, PRESENCE_EXPIRATION_SECONDS, managerId);
      });

      subscribeForRemotePresenceChanges(presenceKey);
    }
  }

  public void renewPresence(final UUID accountUuid, final long deviceId) {
    renewPresenceScript.execute(List.of(getPresenceKey(accountUuid, deviceId)),
        List.of(managerId, String.valueOf(PRESENCE_EXPIRATION_SECONDS)));
  }

  public void disconnectAllPresences(final UUID accountUuid, final List<Long> deviceIds) {

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

  public void disconnectPresence(final UUID accountUuid, final long deviceId) {
    disconnectAllPresences(accountUuid, List.of(deviceId));
  }

  private void displacePresence(final String presenceKey, final boolean connectedElsewhere) {
    final DisplacedPresenceListener displacementListener = displacementListenersByPresenceKey.get(presenceKey);

    if (displacementListener != null) {
      displacementListener.handleDisplacement(connectedElsewhere);
    }

    clearPresence(presenceKey);
  }

  public boolean isPresent(final UUID accountUuid, final long deviceId) {
    try (final Timer.Context ignored = checkPresenceTimer.time()) {
      return presenceCluster.withCluster(connection ->
          connection.sync().exists(getPresenceKey(accountUuid, deviceId))) == 1;
    }
  }

  public boolean isLocallyPresent(final UUID accountUuid, final long deviceId) {
    return displacementListenersByPresenceKey.containsKey(getPresenceKey(accountUuid, deviceId));
  }

  public boolean clearPresence(final UUID accountUuid, final long deviceId) {
    return clearPresence(getPresenceKey(accountUuid, deviceId));
  }

  private boolean clearPresence(final String presenceKey) {
    try (final Timer.Context ignored = clearPresenceTimer.time()) {
      displacementListenersByPresenceKey.remove(presenceKey);
      unsubscribeFromRemotePresenceChanges(presenceKey);

      final boolean removed = clearPresenceScript.execute(List.of(presenceKey), List.of(managerId)) != null;
      presenceCluster.useCluster(connection -> connection.sync().srem(connectedClientSetKey, presenceKey));

      return removed;
    }
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
    try (final Timer.Context ignored = prunePeersTimer.time()) {
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
            pruneClientMeter.mark();
          }

          presenceCluster.useCluster(connection -> {
            connection.sync().del(connectedClientsKey);
            connection.sync().srem(MANAGER_SET_KEY, peerId);
          });
        }
      }
    }
  }

  @Override
  public void message(final RedisClusterNode node, final String channel, final String message) {
    pubSubMessageMeter.mark();

    if (channel.startsWith("__keyspace@0__:presence::{")) {
      if ("set".equals(message) || "del".equals(message)) {
        // for "set", another process has overwritten this presence key, which means the client has connected to another host.
        // for "del", another process has indicated the client should be disconnected
        final boolean connectedElsewhere = "set".equals(message);

        // At this point, we're on a Lettuce IO thread and need to dispatch to a separate thread before making
        // synchronous Lettuce calls to avoid deadlocking.
        keyspaceNotificationExecutorService.execute(() -> {
          try {
            displacePresence(channel.substring("__keyspace@0__:".length()), connectedElsewhere);
            remoteDisplacementMeter.mark();
          } catch (final Exception e) {
            log.warn("Error displacing presence", e);
          }
        });
      }
    }
  }

  @VisibleForTesting
  String getManagerId() {
    return managerId;
  }

  @VisibleForTesting
  static String getPresenceKey(final UUID accountUuid, final long deviceId) {
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
