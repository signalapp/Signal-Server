package org.whispersystems.textsecuregcm.metrics;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PushLatencyManagerTest {

    private PushLatencyManager pushLatencyManager;

    private RedisServer        redisServer;
    private Jedis              jedis;

    @Before
    public void setUp() {
        redisServer                                  = new RedisServer(6379);
        jedis                                        = new Jedis("localhost", 6379);

        final ReplicatedJedisPool jedisPool          = mock(ReplicatedJedisPool.class);
        final ThreadPoolExecutor  threadPoolExecutor = mock(ThreadPoolExecutor.class);

        pushLatencyManager                           = new PushLatencyManager(jedisPool, threadPoolExecutor);

        redisServer.start();

        when(jedisPool.getWriteResource()).thenReturn(jedis);

        doAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(threadPoolExecutor).execute(any(Runnable.class));
    }

    @After
    public void tearDown() {
        redisServer.stop();
    }

    @Test
    public void testGetLatency() {
        final String accountNumber = "mock-account";
        final long deviceId = 1;

        final long expectedLatency     = 1234;
        final long pushSentTimestamp   = System.currentTimeMillis();
        final long clearQueueTimestamp = pushSentTimestamp + expectedLatency;

        {
            assertFalse(pushLatencyManager.getLatencyAndClearTimestamp(accountNumber, deviceId, System.currentTimeMillis()).isPresent());
        }

        {
            pushLatencyManager.recordPushSent(accountNumber, deviceId, pushSentTimestamp);

            assertEquals(Optional.of(expectedLatency),
                    pushLatencyManager.getLatencyAndClearTimestamp(accountNumber, deviceId, clearQueueTimestamp));

            assertFalse(pushLatencyManager.getLatencyAndClearTimestamp(accountNumber, deviceId, System.currentTimeMillis()).isPresent());
        }
    }

    @Test
    public void testThreadPoolDoesNotBlock() throws InterruptedException {
        final AtomicBoolean       canReturnJedisInstance     = new AtomicBoolean(false);
        final ReplicatedJedisPool blockingJedisPool          = mock(ReplicatedJedisPool.class);
        final PushLatencyManager  blockingPushLatencyManager = new PushLatencyManager(blockingJedisPool);

        // One unqueued execution (single-thread pool) plus a full queue
        final CountDownLatch countDownLatch = new CountDownLatch(PushLatencyManager.QUEUE_SIZE + 1);

        when(blockingJedisPool.getWriteResource()).thenAnswer((Answer<Jedis>)invocationOnMock -> {
            synchronized (canReturnJedisInstance) {
                while (!canReturnJedisInstance.get()) {
                    canReturnJedisInstance.wait();
                }
            }

            try {
                return jedis;
            } finally {
                countDownLatch.countDown();
            }
        });

        for (int i = 0; i < PushLatencyManager.QUEUE_SIZE * 2; i++) {
            blockingPushLatencyManager.recordPushSent("account-" + i, 1);
        }

        synchronized (canReturnJedisInstance) {
            canReturnJedisInstance.set(true);
            canReturnJedisInstance.notifyAll();
        }

        if (!countDownLatch.await(5, TimeUnit.SECONDS)) {
            fail("Timed out waiting for countdown latch");
        }
    }
}
