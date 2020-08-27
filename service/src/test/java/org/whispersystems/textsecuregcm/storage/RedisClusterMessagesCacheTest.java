package org.whispersystems.textsecuregcm.storage;

import com.google.protobuf.ByteString;
import io.lettuce.core.cluster.SlotHash;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitParamsRunner.class)
public class RedisClusterMessagesCacheTest extends AbstractRedisClusterTest {

    private ExecutorService           notificationExecutorService;
    private RedisClusterMessagesCache messagesCache;

    private final Random random          = new Random();
    private       long   serialTimestamp = 0;

    private static final String DESTINATION_ACCOUNT   = "+18005551234";
    private static final UUID   DESTINATION_UUID      = UUID.randomUUID();
    private static final int    DESTINATION_DEVICE_ID = 7;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        getRedisCluster().useCluster(connection -> connection.sync().masters().commands().configSet("notify-keyspace-events", "K$gz"));

        notificationExecutorService = Executors.newSingleThreadExecutor();
        messagesCache               = new RedisClusterMessagesCache(getRedisCluster(), notificationExecutorService);

        messagesCache.start();
    }

    @Override
    public void tearDown() throws Exception {
        messagesCache.stop();

        notificationExecutorService.shutdown();
        notificationExecutorService.awaitTermination(1, TimeUnit.SECONDS);

        super.tearDown();
    }


    @Test
    @Parameters({"true", "false"})
    public void testInsert(final boolean sealedSender) {
        final UUID messageGuid = UUID.randomUUID();
        assertTrue(messagesCache.insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, generateRandomMessage(messageGuid, sealedSender)) > 0);
    }

    @Test
    @Parameters({"true", "false"})
    public void testRemoveById(final boolean sealedSender) {
        final UUID                   messageGuid = UUID.randomUUID();
        final MessageProtos.Envelope message     = generateRandomMessage(messageGuid, sealedSender);

        final long                            messageId           = messagesCache.insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);
        final Optional<OutgoingMessageEntity> maybeRemovedMessage = messagesCache.remove(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageId);

        assertTrue(maybeRemovedMessage.isPresent());
        assertEquals(RedisClusterMessagesCache.constructEntityFromEnvelope(messageId, message), maybeRemovedMessage.get());
        assertEquals(Optional.empty(), messagesCache.remove(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageId));
    }

    @Test
    public void testRemoveBySender() {
        final UUID                   messageGuid = UUID.randomUUID();
        final MessageProtos.Envelope message     = generateRandomMessage(messageGuid, false);

        messagesCache.insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);
        final Optional<OutgoingMessageEntity> maybeRemovedMessage = messagesCache.remove(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, message.getSource(), message.getTimestamp());

        assertTrue(maybeRemovedMessage.isPresent());
        assertEquals(RedisClusterMessagesCache.constructEntityFromEnvelope(0, message), maybeRemovedMessage.get());
        assertEquals(Optional.empty(), messagesCache.remove(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, message.getSource(), message.getTimestamp()));
    }

    @Test
    @Parameters({"true", "false"})
    public void testRemoveByUUID(final boolean sealedSender) {
        final UUID messageGuid = UUID.randomUUID();

        assertEquals(Optional.empty(), messagesCache.remove(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageGuid));

        final MessageProtos.Envelope message = generateRandomMessage(messageGuid, sealedSender);

        messagesCache.insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);
        final Optional<OutgoingMessageEntity> maybeRemovedMessage = messagesCache.remove(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageGuid);

        assertTrue(maybeRemovedMessage.isPresent());
        assertEquals(RedisClusterMessagesCache.constructEntityFromEnvelope(0, message), maybeRemovedMessage.get());
    }

    @Test
    @Parameters({"true", "false"})
    public void testGetMessages(final boolean sealedSender) {
        final int messageCount = 100;

        final List<OutgoingMessageEntity> expectedMessages = new ArrayList<>(messageCount);

        for (int i = 0; i < messageCount; i++) {
            final UUID                   messageGuid = UUID.randomUUID();
            final MessageProtos.Envelope message     = generateRandomMessage(messageGuid, sealedSender);
            final long                   messageId   = messagesCache.insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);

            expectedMessages.add(RedisClusterMessagesCache.constructEntityFromEnvelope(messageId, message));
        }

        assertEquals(expectedMessages, messagesCache.get(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
    }

    @Test
    @Parameters({"true", "false"})
    public void testClearQueueForDevice(final boolean sealedSender) {
        final int messageCount = 100;

        for (final int deviceId : new int[] { DESTINATION_DEVICE_ID, DESTINATION_DEVICE_ID + 1 }) {
            for (int i = 0; i < messageCount; i++) {
                final UUID                   messageGuid = UUID.randomUUID();
                final MessageProtos.Envelope message     = generateRandomMessage(messageGuid, sealedSender);

                messagesCache.insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, deviceId, message);
            }
        }

        messagesCache.clear(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID);

        assertEquals(Collections.emptyList(), messagesCache.get(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
        assertEquals(messageCount, messagesCache.get(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID + 1, messageCount).size());
    }

    @Test
    @Parameters({"true", "false"})
    public void testClearQueueForAccount(final boolean sealedSender) {
        final int messageCount = 100;

        for (final int deviceId : new int[] { DESTINATION_DEVICE_ID, DESTINATION_DEVICE_ID + 1 }) {
            for (int i = 0; i < messageCount; i++) {
                final UUID                   messageGuid = UUID.randomUUID();
                final MessageProtos.Envelope message     = generateRandomMessage(messageGuid, sealedSender);

                messagesCache.insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, deviceId, message);
            }
        }

        messagesCache.clear(DESTINATION_ACCOUNT, DESTINATION_UUID);

        assertEquals(Collections.emptyList(), messagesCache.get(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
        assertEquals(Collections.emptyList(), messagesCache.get(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID + 1, messageCount));
    }

    protected MessageProtos.Envelope generateRandomMessage(final UUID messageGuid, final boolean sealedSender) {
        final MessageProtos.Envelope.Builder envelopeBuilder = MessageProtos.Envelope.newBuilder()
                .setTimestamp(serialTimestamp++)
                .setServerTimestamp(serialTimestamp++)
                .setContent(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(256)))
                .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
                .setServerGuid(messageGuid.toString());

        if (!sealedSender) {
            envelopeBuilder.setSourceDevice(random.nextInt(256))
                    .setSource("+1" + RandomStringUtils.randomNumeric(10));
        }

        return envelopeBuilder.build();
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
