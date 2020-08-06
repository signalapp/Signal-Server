package org.whispersystems.textsecuregcm.storage;

import org.junit.After;
import org.junit.Before;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.providers.RedisClientFactory;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import redis.embedded.RedisServer;

import java.util.List;

import static org.mockito.Mockito.mock;

public class MessagesCacheTest extends AbstractMessagesCacheTest {

    private RedisServer   redisServer;
    private MessagesCache messagesCache;

    @Before
    public void setUp() throws Exception {
        redisServer = new RedisServer(AbstractRedisClusterTest.getNextRedisClusterPort());
        redisServer.start();

        final String              redisUrl      = String.format("redis://127.0.0.1:%d", redisServer.ports().get(0));
        final RedisClientFactory  clientFactory = new RedisClientFactory("message-cache-test", redisUrl, List.of(redisUrl), new CircuitBreakerConfiguration());
        final ReplicatedJedisPool jedisPool     = clientFactory.getRedisClientPool();

        messagesCache = new MessagesCache(jedisPool);
    }

    @After
    public void tearDown() {
        redisServer.stop();
    }

    @Override
    protected UserMessagesCache getMessagesCache() {
        return messagesCache;
    }
}
