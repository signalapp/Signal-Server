/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;

class ClientPresenceManagerTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private ScheduledExecutorService presenceRenewalExecutorService;
  private ClientPresenceManager clientPresenceManager;

  private static final DisplacedPresenceListener NO_OP = connectedElsewhere -> {
  };

  @BeforeEach
  void setUp() throws Exception {

    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection -> {
      connection.sync().flushall();
      connection.sync().upstream().commands().configSet("notify-keyspace-events", "K$glz");
    });

    presenceRenewalExecutorService = Executors.newSingleThreadScheduledExecutor();
    clientPresenceManager = new ClientPresenceManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        presenceRenewalExecutorService,
        presenceRenewalExecutorService);
  }

  @AfterEach
  public void tearDown() throws Exception {
    presenceRenewalExecutorService.shutdown();
    presenceRenewalExecutorService.awaitTermination(1, TimeUnit.MINUTES);

    clientPresenceManager.stop();
  }

  @Test
  void testIsPresent() {
    final UUID accountUuid = UUID.randomUUID();
    final long deviceId = 1;

    assertFalse(clientPresenceManager.isPresent(accountUuid, deviceId));

    clientPresenceManager.setPresent(accountUuid, deviceId, NO_OP);
    assertTrue(clientPresenceManager.isPresent(accountUuid, deviceId));
  }

  @Test
  void testIsLocallyPresent() {
    final UUID accountUuid = UUID.randomUUID();
    final long deviceId = 1;

    assertFalse(clientPresenceManager.isLocallyPresent(accountUuid, deviceId));

    clientPresenceManager.setPresent(accountUuid, deviceId, NO_OP);
    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection -> connection.sync().flushall());

    assertTrue(clientPresenceManager.isLocallyPresent(accountUuid, deviceId));
  }

  @Test
  void testLocalDisplacement() {
    final UUID accountUuid = UUID.randomUUID();
    final long deviceId = 1;

    final AtomicInteger displacementCounter = new AtomicInteger(0);
    final DisplacedPresenceListener displacementListener = connectedElsewhere -> displacementCounter.incrementAndGet();

    clientPresenceManager.setPresent(accountUuid, deviceId, displacementListener);

    assertEquals(0, displacementCounter.get());

    clientPresenceManager.setPresent(accountUuid, deviceId, displacementListener);

    assertEquals(1, displacementCounter.get());
  }

  @Test
  void testRemoteDisplacement() {
    final UUID accountUuid = UUID.randomUUID();
    final long deviceId = 1;

    final CompletableFuture<?> displaced = new CompletableFuture<>();

    clientPresenceManager.start();

    clientPresenceManager.setPresent(accountUuid, deviceId, connectedElsewhere -> displaced.complete(null));

    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(
        connection -> connection.sync().set(ClientPresenceManager.getPresenceKey(accountUuid, deviceId),
            UUID.randomUUID().toString()));

    assertTimeoutPreemptively(Duration.ofSeconds(10), displaced::join);
  }

  @Test
  void testRemoteDisplacementAfterTopologyChange() {
    final UUID accountUuid = UUID.randomUUID();
    final long deviceId = 1;

    final CompletableFuture<?> displaced = new CompletableFuture<>();

    clientPresenceManager.start();

    clientPresenceManager.setPresent(accountUuid, deviceId, connectedElsewhere -> displaced.complete(null));

    clientPresenceManager.getPubSubConnection()
        .usePubSubConnection(connection -> connection.getResources().eventBus()
            .publish(new ClusterTopologyChangedEvent(List.of(), List.of())));

    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(
        connection -> connection.sync().set(ClientPresenceManager.getPresenceKey(accountUuid, deviceId),
            UUID.randomUUID().toString()));

    assertTimeoutPreemptively(Duration.ofSeconds(10), displaced::join);
  }

  @Test
  void testClearPresence() {
    final UUID accountUuid = UUID.randomUUID();
    final long deviceId = 1;

    assertFalse(clientPresenceManager.isPresent(accountUuid, deviceId));

    clientPresenceManager.setPresent(accountUuid, deviceId, NO_OP);
    assertTrue(clientPresenceManager.clearPresence(accountUuid, deviceId));

    clientPresenceManager.setPresent(accountUuid, deviceId, NO_OP);
    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(
        connection -> connection.sync().set(ClientPresenceManager.getPresenceKey(accountUuid, deviceId),
            UUID.randomUUID().toString()));

    assertFalse(clientPresenceManager.clearPresence(accountUuid, deviceId));
  }

  @Test
  void testPruneMissingPeers() {
    final String presentPeerId = UUID.randomUUID().toString();
    final String missingPeerId = UUID.randomUUID().toString();

    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection -> {
      connection.sync().sadd(ClientPresenceManager.MANAGER_SET_KEY, presentPeerId);
      connection.sync().sadd(ClientPresenceManager.MANAGER_SET_KEY, missingPeerId);
    });

    for (int i = 0; i < 10; i++) {
      addClientPresence(presentPeerId);
      addClientPresence(missingPeerId);
    }

    clientPresenceManager.getPubSubConnection().usePubSubConnection(
        connection -> connection.sync().upstream().commands()
            .subscribe(ClientPresenceManager.getManagerPresenceChannel(presentPeerId)));
    clientPresenceManager.pruneMissingPeers();

    assertEquals(1, (long) REDIS_CLUSTER_EXTENSION.getRedisCluster().withCluster(
        connection -> connection.sync().exists(ClientPresenceManager.getConnectedClientSetKey(presentPeerId))));
    assertTrue(REDIS_CLUSTER_EXTENSION.getRedisCluster().withCluster(
        (Function<StatefulRedisClusterConnection<String, String>, Boolean>) connection -> connection.sync()
            .sismember(ClientPresenceManager.MANAGER_SET_KEY, presentPeerId)));

    assertEquals(0, (long) REDIS_CLUSTER_EXTENSION.getRedisCluster().withCluster(
        connection -> connection.sync().exists(ClientPresenceManager.getConnectedClientSetKey(missingPeerId))));
    assertFalse(REDIS_CLUSTER_EXTENSION.getRedisCluster().withCluster(
        (Function<StatefulRedisClusterConnection<String, String>, Boolean>) connection -> connection.sync()
            .sismember(ClientPresenceManager.MANAGER_SET_KEY, missingPeerId)));
  }

  @Test
  void testInitialPresenceExpiration() {
    final UUID accountUuid = UUID.randomUUID();
    final long deviceId = 1;

    clientPresenceManager.setPresent(accountUuid, deviceId, NO_OP);

    {
      final int ttl = REDIS_CLUSTER_EXTENSION.getRedisCluster().withCluster(connection ->
          connection.sync().ttl(ClientPresenceManager.getPresenceKey(accountUuid, deviceId)).intValue());

      assertTrue(ttl > 0);
    }
  }

  @Test
  void testRenewPresence() {
    final UUID accountUuid = UUID.randomUUID();
    final long deviceId = 1;

    final String presenceKey = ClientPresenceManager.getPresenceKey(accountUuid, deviceId);

    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection ->
        connection.sync().set(presenceKey, clientPresenceManager.getManagerId()));

    {
      final int ttl = REDIS_CLUSTER_EXTENSION.getRedisCluster().withCluster(connection ->
          connection.sync().ttl(presenceKey).intValue());

      assertEquals(-1, ttl);
    }

    clientPresenceManager.renewPresence(accountUuid, deviceId);

    {
      final int ttl = REDIS_CLUSTER_EXTENSION.getRedisCluster().withCluster(connection ->
          connection.sync().ttl(presenceKey).intValue());

      assertTrue(ttl > 0);
    }
  }

  @Test
  void testExpiredPresence() {
    final UUID accountUuid = UUID.randomUUID();
    final long deviceId = 1;

    clientPresenceManager.setPresent(accountUuid, deviceId, NO_OP);

    assertTrue(clientPresenceManager.isPresent(accountUuid, deviceId));

    // Hackily set this key to expire immediately
    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection ->
        connection.sync().expire(ClientPresenceManager.getPresenceKey(accountUuid, deviceId), 0));

    assertFalse(clientPresenceManager.isPresent(accountUuid, deviceId));
  }

  private void addClientPresence(final String managerId) {
    final String clientPresenceKey = ClientPresenceManager.getPresenceKey(UUID.randomUUID(), 7);

    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection -> {
      connection.sync().set(clientPresenceKey, managerId);
      connection.sync().sadd(ClientPresenceManager.getConnectedClientSetKey(managerId), clientPresenceKey);
    });
  }

  @Test
  void testClearAllOnStop() {
    final int localAccounts = 10;
    final UUID[] localUuids = new UUID[localAccounts];
    final long[] localDeviceIds = new long[localAccounts];

    for (int i = 0; i < localAccounts; i++) {
      localUuids[i] = UUID.randomUUID();
      localDeviceIds[i] = i;

      clientPresenceManager.setPresent(localUuids[i], localDeviceIds[i], NO_OP);
    }

    final UUID displacedAccountUuid = UUID.randomUUID();
    final long displacedAccountDeviceId = 7;

    clientPresenceManager.setPresent(displacedAccountUuid, displacedAccountDeviceId, NO_OP);
    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection -> connection.sync()
        .set(ClientPresenceManager.getPresenceKey(displacedAccountUuid, displacedAccountDeviceId),
            UUID.randomUUID().toString()));

    clientPresenceManager.stop();

    for (int i = 0; i < localAccounts; i++) {
      localUuids[i] = UUID.randomUUID();
      localDeviceIds[i] = i;

      assertFalse(clientPresenceManager.isPresent(localUuids[i], localDeviceIds[i]));
    }

    assertTrue(clientPresenceManager.isPresent(displacedAccountUuid, displacedAccountDeviceId));
  }

  @Nested
  class MultiServerTest {

    private ClientPresenceManager server1;
    private ClientPresenceManager server2;

    @BeforeEach
    void setup() throws Exception {

      REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection -> {
        connection.sync().flushall();
        connection.sync().upstream().commands().configSet("notify-keyspace-events", "K$glz");
      });

      final ScheduledExecutorService scheduledExecutorService1 = mock(ScheduledExecutorService.class);
      final ExecutorService keyspaceNotificationExecutorService1 = Executors.newSingleThreadExecutor();
      server1 = new ClientPresenceManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
          scheduledExecutorService1, keyspaceNotificationExecutorService1);

      final ScheduledExecutorService scheduledExecutorService2 = mock(ScheduledExecutorService.class);
      final ExecutorService keyspaceNotificationExecutorService2 = Executors.newSingleThreadExecutor();
      server2 = new ClientPresenceManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
          scheduledExecutorService2, keyspaceNotificationExecutorService2);

      server1.start();
      server2.start();
    }

    @AfterEach
    void teardown() {
      server2.stop();
      server1.stop();
    }

    @Test
    void testSetPresentRemotely() {
      final UUID uuid1 = UUID.randomUUID();
      final long deviceId = 1L;

      final CompletableFuture<?> displaced = new CompletableFuture<>();
      final DisplacedPresenceListener listener1 = connectedElsewhere -> displaced.complete(null);
      server1.setPresent(uuid1, deviceId, listener1);

      server2.setPresent(uuid1, deviceId, connectedElsewhere -> {});

      assertTimeoutPreemptively(Duration.ofSeconds(10), displaced::join);
    }

    @Test
    void testDisconnectPresenceLocally() {
      final UUID uuid1 = UUID.randomUUID();
      final long deviceId = 1L;

      final CompletableFuture<?> displaced = new CompletableFuture<>();
      final DisplacedPresenceListener listener1 = connectedElsewhere -> displaced.complete(null);
      server1.setPresent(uuid1, deviceId, listener1);

      server1.disconnectPresence(uuid1, deviceId);

      assertTimeoutPreemptively(Duration.ofSeconds(10), displaced::join);
    }

    @Test
    void testDisconnectPresenceRemotely() {
      final UUID uuid1 = UUID.randomUUID();
      final long deviceId = 1L;

      final CompletableFuture<?> displaced = new CompletableFuture<>();
      final DisplacedPresenceListener listener1 = connectedElsewhere -> displaced.complete(null);
      server1.setPresent(uuid1, deviceId, listener1);

      server2.disconnectPresence(uuid1, deviceId);

      assertTimeoutPreemptively(Duration.ofSeconds(10), displaced::join);
    }
  }
}
