/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.lettuce.core.RedisClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.providers.RedisClientFactory;
import redis.embedded.RedisServer;

import java.time.Duration;
import java.util.List;

import static org.junit.Assume.assumeFalse;

public class AbstractRedisSingletonTest {

    private static RedisServer redisServer;

    private FaultTolerantRedisClient redisClient;
    private ReplicatedJedisPool replicatedJedisPool;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
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
        final String redisUrl = String.format("redis://127.0.0.1:%d", redisServer.ports().get(0));

        redisClient = new FaultTolerantRedisClient("test-client",
                RedisClient.create(redisUrl),
                Duration.ofSeconds(2),
                new CircuitBreakerConfiguration());

        replicatedJedisPool = new RedisClientFactory("test-pool",
                redisUrl,
                List.of(redisUrl),
                new CircuitBreakerConfiguration()).getRedisClientPool();

        redisClient.useClient(connection -> connection.sync().flushall());
    }

    protected FaultTolerantRedisClient getRedisClient() {
        return redisClient;
    }

    protected ReplicatedJedisPool getJedisPool() {
        return replicatedJedisPool;
    }

    @After
    public void tearDown() throws Exception {
        redisClient.shutdown();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        redisServer.stop();
    }
}
