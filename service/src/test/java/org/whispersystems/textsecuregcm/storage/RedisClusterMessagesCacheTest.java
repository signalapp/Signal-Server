package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.cluster.SlotHash;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RedisClusterMessagesCacheTest extends AbstractMessagesCacheTest {

    private static final String DESTINATION_ACCOUNT   = "+18005551234";
    private static final UUID   DESTINATION_UUID      = UUID.randomUUID();
    private static final int    DESTINATION_DEVICE_ID = 7;

    private ExecutorService           notificationExecutorService;
    private RedisClusterMessagesCache messagesCache;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        getRedisCluster().useCluster(connection -> connection.sync().masters().commands().configSet("notify-keyspace-events", "K$gz"));

        notificationExecutorService = Executors.newSingleThreadExecutor();
        messagesCache               = new RedisClusterMessagesCache(getRedisCluster(), notificationExecutorService);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        notificationExecutorService.shutdown();
        notificationExecutorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Override
    protected UserMessagesCache getMessagesCache() {
        return messagesCache;
    }

    @Test
    @Parameters({"true", "false"})
    public void testInsertWithPrescribedId(final boolean sealedSender) {
        final UUID firstMessageGuid  = UUID.randomUUID();
        final UUID secondMessageGuid = UUID.randomUUID();
        final long messageId         = 74;

        assertEquals(messageId, messagesCache.insert(firstMessageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, generateRandomMessage(firstMessageGuid, sealedSender), messageId));
        assertEquals(messageId + 1, messagesCache.insert(secondMessageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, generateRandomMessage(secondMessageGuid, sealedSender)));
    }

    @Test
    public void testClearNullUuid() {
        // We're happy as long as this doesn't throw an exception
        messagesCache.clear(DESTINATION_ACCOUNT, null);
    }

    @Test
    public void testGetAccountFromQueueName() {
        assertEquals(DESTINATION_UUID,
                     RedisClusterMessagesCache.getAccountUuidFromQueueName(new String(RedisClusterMessagesCache.getMessageQueueKey(DESTINATION_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8)));
    }

    @Test
    public void testGetDeviceIdFromQueueName() {
        assertEquals(DESTINATION_DEVICE_ID,
                     RedisClusterMessagesCache.getDeviceIdFromQueueName(new String(RedisClusterMessagesCache.getMessageQueueKey(DESTINATION_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8)));
    }

    @Test
    public void testGetQueueNameFromKeyspaceChannel() {
        assertEquals("1b363a31-a429-4fb6-8959-984a025e72ff::7",
                     RedisClusterMessagesCache.getQueueNameFromKeyspaceChannel("__keyspace@0__:user_queue::{1b363a31-a429-4fb6-8959-984a025e72ff::7}"));
    }

    @Test
    @Parameters({"true", "false"})
    public void testGetQueuesToPersist(final boolean sealedSender) {
        final UUID messageGuid  = UUID.randomUUID();

        messagesCache.insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, generateRandomMessage(messageGuid, sealedSender));
        final int slot = SlotHash.getSlot(DESTINATION_UUID.toString() + "::" + DESTINATION_DEVICE_ID);

        assertTrue(messagesCache.getQueuesToPersist(slot + 1, Instant.now().plusSeconds(60), 100).isEmpty());

        final List<String> queues = messagesCache.getQueuesToPersist(slot, Instant.now().plusSeconds(60), 100);

        assertEquals(1, queues.size());
        assertEquals(DESTINATION_UUID, RedisClusterMessagesCache.getAccountUuidFromQueueName(queues.get(0)));
        assertEquals(DESTINATION_DEVICE_ID, RedisClusterMessagesCache.getDeviceIdFromQueueName(queues.get(0)));
    }

    @Test(timeout = 5_000L)
    @Ignore
    public void testNotifyListenerNewMessage() throws InterruptedException {
        final AtomicBoolean notified    = new AtomicBoolean(false);
        final UUID          messageGuid = UUID.randomUUID();

        final MessageAvailabilityListener listener = new MessageAvailabilityListener() {
            @Override
            public void handleNewMessagesAvailable() {
                synchronized (notified) {
                    notified.set(true);
                    notified.notifyAll();
                }
            }

            @Override
            public void handleMessagesPersisted() {
            }
        };

        messagesCache.addMessageAvailabilityListener(DESTINATION_UUID, DESTINATION_DEVICE_ID, listener);
        messagesCache.insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, generateRandomMessage(messageGuid, true));

        synchronized (notified) {
            while (!notified.get()) {
                notified.wait();
            }
        }

        assertTrue(notified.get());
    }

    @Test(timeout = 5_000L)
    @Ignore
    public void testNotifyListenerPersisted() throws InterruptedException {
        final AtomicBoolean notified    = new AtomicBoolean(false);

        final MessageAvailabilityListener listener = new MessageAvailabilityListener() {
            @Override
            public void handleNewMessagesAvailable() {
            }

            @Override
            public void handleMessagesPersisted() {
                synchronized (notified) {
                    notified.set(true);
                    notified.notifyAll();
                }
            }
        };

        messagesCache.addMessageAvailabilityListener(DESTINATION_UUID, DESTINATION_DEVICE_ID, listener);

        messagesCache.lockQueueForPersistence(RedisClusterMessagesCache.getQueueName(DESTINATION_UUID, DESTINATION_DEVICE_ID));
        messagesCache.unlockQueueForPersistence(RedisClusterMessagesCache.getQueueName(DESTINATION_UUID, DESTINATION_DEVICE_ID));

        synchronized (notified) {
            while (!notified.get()) {
                notified.wait();
            }
        }

        assertTrue(notified.get());
    }
}
