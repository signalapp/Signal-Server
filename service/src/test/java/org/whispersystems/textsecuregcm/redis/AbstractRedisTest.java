package org.whispersystems.textsecuregcm.redis;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.providers.RedisClientFactory;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.util.List;

public abstract class AbstractRedisTest {

    private static RedisServer redisServer;

    private ReplicatedJedisPool replicatedJedisPool;

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        redisServer = new RedisServer(getNextPort());
        redisServer.start();
    }

    @Before
    public void setUp() throws URISyntaxException {
        final String redisUrl = "redis://127.0.0.1:" + redisServer.ports().get(0);
        replicatedJedisPool = new RedisClientFactory("test-pool", redisUrl, List.of(redisUrl), new CircuitBreakerConfiguration()).getRedisClientPool();
    }

    protected ReplicatedJedisPool getReplicatedJedisPool() {
        return replicatedJedisPool;
    }

    @AfterClass
    public static void tearDownAfterClass() {
        redisServer.stop();
    }

    private static int getNextPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(false);
            return socket.getLocalPort();
        }
    }
}
