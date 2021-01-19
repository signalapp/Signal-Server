/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import io.lettuce.core.cluster.SlotHash;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.providers.RedisClientFactory;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;
import org.whispersystems.textsecuregcm.redis.RedisException;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Pair;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApnFallbackManagerTest extends AbstractRedisClusterTest {

    private Account account;
    private Device device;

    private APNSender apnSender;

    private ApnFallbackManager apnFallbackManager;

    private static RedisServer redisServer;

    private static final UUID   ACCOUNT_UUID   = UUID.randomUUID();
    private static final String ACCOUNT_NUMBER = "+18005551234";
    private static final long   DEVICE_ID      = 1L;
    private static final String VOIP_APN_ID = RandomStringUtils.randomAlphanumeric(32);

    @BeforeClass
    public static void setUpRedisSingleton() throws Exception {
        assumeFalse(System.getProperty("os.name").equalsIgnoreCase("windows"));

        redisServer = RedisServer.builder()
                .setting("appendonly no")
                .setting("dir " + System.getProperty("java.io.tmpdir"))
                .port(AbstractRedisClusterTest.getNextRedisClusterPort())
                .build();

        redisServer.start();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        final String redisUrl = String.format("redis://127.0.0.1:%d", redisServer.ports().get(0));

        final ReplicatedJedisPool replicatedJedisPool = new RedisClientFactory("test-pool",
                redisUrl,
                List.of(redisUrl),
                new CircuitBreakerConfiguration()).getRedisClientPool();

        try (final Jedis jedis = replicatedJedisPool.getWriteResource()) {
            jedis.flushAll();
        }

        device = mock(Device.class);
        when(device.getId()).thenReturn(DEVICE_ID);
        when(device.getVoipApnId()).thenReturn(VOIP_APN_ID);
        when(device.getLastSeen()).thenReturn(System.currentTimeMillis());

        account = mock(Account.class);
        when(account.getUuid()).thenReturn(ACCOUNT_UUID);
        when(account.getNumber()).thenReturn(ACCOUNT_NUMBER);
        when(account.getDevice(DEVICE_ID)).thenReturn(Optional.of(device));

        final AccountsManager accountsManager = mock(AccountsManager.class);
        when(accountsManager.get(ACCOUNT_NUMBER)).thenReturn(Optional.of(account));
        when(accountsManager.get(ACCOUNT_UUID)).thenReturn(Optional.of(account));

        apnSender = mock(APNSender.class);

        apnFallbackManager = new ApnFallbackManager(replicatedJedisPool, getRedisCluster(), apnSender, accountsManager);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @AfterClass
    public static void tearDownRedisSingleton() {
        redisServer.stop();
    }

    @Test
    public void testClusterInsert() throws RedisException {
        final String endpoint = apnFallbackManager.getClusterEndpointKey(account, device);

        assertTrue(apnFallbackManager.getPendingDestinationsFromClusterCache(SlotHash.getSlot(endpoint), 1).isEmpty());

        apnFallbackManager.schedule(account, device, System.currentTimeMillis() - 30_000);

        final List<String> pendingDestinations = apnFallbackManager.getPendingDestinationsFromClusterCache(SlotHash.getSlot(endpoint), 2);
        assertEquals(1, pendingDestinations.size());

        final Optional<Pair<String, Long>> maybeUuidAndDeviceId = ApnFallbackManager.getSeparated(pendingDestinations.get(0));

        assertTrue(maybeUuidAndDeviceId.isPresent());
        assertEquals(ACCOUNT_UUID.toString(), maybeUuidAndDeviceId.get().first());
        assertEquals(DEVICE_ID, (long)maybeUuidAndDeviceId.get().second());

        assertTrue(apnFallbackManager.getPendingDestinationsFromClusterCache(SlotHash.getSlot(endpoint), 1).isEmpty());
    }

    @Test
    public void testProcessNextSlot() throws RedisException {
        final ApnFallbackManager.ClusterCacheWorker worker = apnFallbackManager.new ClusterCacheWorker();

        apnFallbackManager.schedule(account, device, System.currentTimeMillis() - 30_000);

        final int slot = SlotHash.getSlot(apnFallbackManager.getClusterEndpointKey(account, device));
        final int previousSlot = (slot + SlotHash.SLOT_COUNT - 1) % SlotHash.SLOT_COUNT;

        getRedisCluster().withCluster(connection -> connection.sync().set(ApnFallbackManager.NEXT_SLOT_TO_PERSIST_KEY, String.valueOf(previousSlot)));

        assertEquals(1, worker.processNextSlot());

        final ArgumentCaptor<ApnMessage> messageCaptor = ArgumentCaptor.forClass(ApnMessage.class);
        verify(apnSender).sendMessage(messageCaptor.capture());

        final ApnMessage message = messageCaptor.getValue();

        assertEquals(VOIP_APN_ID, message.getApnId());
        assertEquals(ACCOUNT_NUMBER, message.getNumber());
        assertEquals(DEVICE_ID, message.getDeviceId());

        assertEquals(0, worker.processNextSlot());
    }
}
