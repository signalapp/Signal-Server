/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClientPresenceManagerTest extends AbstractRedisClusterTest {

    private ScheduledExecutorService presenceRenewalExecutorService;
    private ClientPresenceManager clientPresenceManager;

    private static final DisplacedPresenceListener NO_OP = () -> {};

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        getRedisCluster().useCluster(connection -> {
            connection.sync().flushall();
            connection.sync().masters().commands().configSet("notify-keyspace-events", "K$z");
        });

        presenceRenewalExecutorService = Executors.newSingleThreadScheduledExecutor();
        clientPresenceManager          = new ClientPresenceManager(getRedisCluster(), presenceRenewalExecutorService, presenceRenewalExecutorService);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();

        presenceRenewalExecutorService.shutdown();
        presenceRenewalExecutorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    public void testIsPresent() {
        final UUID accountUuid = UUID.randomUUID();
        final long deviceId    = 1;

        assertFalse(clientPresenceManager.isPresent(accountUuid, deviceId));

        clientPresenceManager.setPresent(accountUuid, deviceId, NO_OP);
        assertTrue(clientPresenceManager.isPresent(accountUuid, deviceId));
    }

    @Test
    public void testIsLocallyPresent() {
        final UUID accountUuid = UUID.randomUUID();
        final long deviceId    = 1;

        assertFalse(clientPresenceManager.isLocallyPresent(accountUuid, deviceId));

        clientPresenceManager.setPresent(accountUuid, deviceId, NO_OP);
        getRedisCluster().useCluster(connection -> connection.sync().flushall());

        assertTrue(clientPresenceManager.isLocallyPresent(accountUuid, deviceId));
    }

    @Test
    public void testLocalDisplacement() {
        final UUID accountUuid = UUID.randomUUID();
        final long deviceId    = 1;

        final AtomicInteger             displacementCounter  = new AtomicInteger(0);
        final DisplacedPresenceListener displacementListener = displacementCounter::incrementAndGet;

        clientPresenceManager.setPresent(accountUuid, deviceId, displacementListener);

        assertEquals(0, displacementCounter.get());

        clientPresenceManager.setPresent(accountUuid, deviceId, displacementListener);

        assertEquals(1, displacementCounter.get());
    }

    @Test(timeout = 10_000)
    public void testRemoteDisplacement() throws InterruptedException {
        final UUID accountUuid = UUID.randomUUID();
        final long deviceId    = 1;

        final AtomicBoolean displaced = new AtomicBoolean(false);

        clientPresenceManager.start();

        try {
            clientPresenceManager.setPresent(accountUuid, deviceId, () -> {
                synchronized (displaced) {
                    displaced.set(true);
                    displaced.notifyAll();
                }
            });

            getRedisCluster().useCluster(connection -> connection.sync().set(ClientPresenceManager.getPresenceKey(accountUuid, deviceId),
                    UUID.randomUUID().toString()));

            synchronized (displaced) {
                while (!displaced.get()) {
                    displaced.wait();
                }
            }
        } finally {
            clientPresenceManager.stop();
        }
    }

    @Test(timeout = 10_000)
    public void testRemoteDisplacementAfterTopologyChange() throws InterruptedException {
        final UUID accountUuid = UUID.randomUUID();
        final long deviceId    = 1;

        final AtomicBoolean displaced = new AtomicBoolean(false);

        clientPresenceManager.start();

        try {
            clientPresenceManager.setPresent(accountUuid, deviceId, () -> {
                synchronized (displaced) {
                    displaced.set(true);
                    displaced.notifyAll();
                }
            });

            clientPresenceManager.getPubSubConnection().usePubSubConnection(connection -> connection.getResources().eventBus().publish(new ClusterTopologyChangedEvent(List.of(), List.of())));

            getRedisCluster().useCluster(connection -> connection.sync().set(ClientPresenceManager.getPresenceKey(accountUuid, deviceId),
                    UUID.randomUUID().toString()));

            synchronized (displaced) {
                while (!displaced.get()) {
                    displaced.wait();
                }
            }
        } finally {
            clientPresenceManager.stop();
        }
    }

    @Test
    public void testClearPresence() {
        final UUID accountUuid = UUID.randomUUID();
        final long deviceId    = 1;

        assertFalse(clientPresenceManager.isPresent(accountUuid, deviceId));

        clientPresenceManager.setPresent(accountUuid, deviceId, NO_OP);
        assertTrue(clientPresenceManager.clearPresence(accountUuid, deviceId));

        clientPresenceManager.setPresent(accountUuid, deviceId, NO_OP);
        getRedisCluster().useCluster(connection -> connection.sync().set(ClientPresenceManager.getPresenceKey(accountUuid, deviceId),
                UUID.randomUUID().toString()));

        assertFalse(clientPresenceManager.clearPresence(accountUuid, deviceId));
    }

    @Test
    public void testPruneMissingPeers() {
        final String presentPeerId = UUID.randomUUID().toString();
        final String missingPeerId = UUID.randomUUID().toString();

        getRedisCluster().useCluster(connection -> {
            connection.sync().sadd(ClientPresenceManager.MANAGER_SET_KEY, presentPeerId);
            connection.sync().sadd(ClientPresenceManager.MANAGER_SET_KEY, missingPeerId);
        });

        for (int i = 0; i < 10; i++) {
            addClientPresence(presentPeerId);
            addClientPresence(missingPeerId);
        }

        clientPresenceManager.getPubSubConnection().usePubSubConnection(connection -> connection.sync().masters().commands().subscribe(ClientPresenceManager.getManagerPresenceChannel(presentPeerId)));
        clientPresenceManager.pruneMissingPeers();

        assertEquals(1, (long)getRedisCluster().withCluster(connection -> connection.sync().exists(ClientPresenceManager.getConnectedClientSetKey(presentPeerId))));
        assertTrue(getRedisCluster().withCluster(connection -> connection.sync().sismember(ClientPresenceManager.MANAGER_SET_KEY, presentPeerId)));

        assertEquals(0, (long)getRedisCluster().withCluster(connection -> connection.sync().exists(ClientPresenceManager.getConnectedClientSetKey(missingPeerId))));
        assertFalse(getRedisCluster().withCluster(connection -> connection.sync().sismember(ClientPresenceManager.MANAGER_SET_KEY, missingPeerId)));
    }

    private void addClientPresence(final String managerId) {
        final String clientPresenceKey = ClientPresenceManager.getPresenceKey(UUID.randomUUID(), 7);

        getRedisCluster().useCluster(connection -> {
            connection.sync().set(clientPresenceKey, managerId);
            connection.sync().sadd(ClientPresenceManager.getConnectedClientSetKey(managerId), clientPresenceKey);
        });
    }

    @Test
    public void testClearAllOnStop() {
        final int    localAccounts  = 10;
        final UUID[] localUuids     = new UUID[localAccounts];
        final long[] localDeviceIds = new long[localAccounts];

        for (int i = 0; i < localAccounts; i++) {
            localUuids[i]     = UUID.randomUUID();
            localDeviceIds[i] = i;

            clientPresenceManager.setPresent(localUuids[i], localDeviceIds[i], NO_OP);
        }

        final UUID displacedAccountUuid     = UUID.randomUUID();
        final long displacedAccountDeviceId = 7;

        clientPresenceManager.setPresent(displacedAccountUuid, displacedAccountDeviceId, NO_OP);
        getRedisCluster().useCluster(connection -> connection.sync().set(ClientPresenceManager.getPresenceKey(displacedAccountUuid, displacedAccountDeviceId),
                                                                              UUID.randomUUID().toString()));

        clientPresenceManager.stop();

        for (int i = 0; i < localAccounts; i++) {
            localUuids[i]     = UUID.randomUUID();
            localDeviceIds[i] = i;

            assertFalse(clientPresenceManager.isPresent(localUuids[i], localDeviceIds[i]));
        }

        assertTrue(clientPresenceManager.isPresent(displacedAccountUuid, displacedAccountDeviceId));
    }
}
