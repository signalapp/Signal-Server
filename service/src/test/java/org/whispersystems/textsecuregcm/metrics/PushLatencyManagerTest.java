package org.whispersystems.textsecuregcm.metrics;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PushLatencyManagerTest extends AbstractRedisClusterTest {

    @Test
    public void testGetLatency() throws ExecutionException, InterruptedException {
        final PushLatencyManager pushLatencyManager  = new PushLatencyManager(getRedisCluster());
        final UUID               accountUuid         = UUID.randomUUID();
        final long               deviceId            = 1;
        final long               expectedLatency     = 1234;
        final long               pushSentTimestamp   = System.currentTimeMillis();
        final long               clearQueueTimestamp = pushSentTimestamp + expectedLatency;

        assertNull(pushLatencyManager.getLatencyAndClearTimestamp(accountUuid, deviceId, System.currentTimeMillis()).get());

        {
            pushLatencyManager.recordPushSent(accountUuid, deviceId, pushSentTimestamp);

            assertEquals(expectedLatency, (long)pushLatencyManager.getLatencyAndClearTimestamp(accountUuid, deviceId, clearQueueTimestamp).get());
            assertNull(pushLatencyManager.getLatencyAndClearTimestamp(accountUuid, deviceId, System.currentTimeMillis()).get());
        }
    }
}
