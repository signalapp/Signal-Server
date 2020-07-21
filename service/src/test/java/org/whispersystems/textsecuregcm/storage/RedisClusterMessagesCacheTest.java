package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.cluster.SlotHash;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RedisClusterMessagesCacheTest extends AbstractMessagesCacheTest {

    private static final String DESTINATION_ACCOUNT   = "+18005551234";
    private static final UUID   DESTINATION_UUID      = UUID.randomUUID();
    private static final int    DESTINATION_DEVICE_ID = 7;

    private RedisClusterMessagesCache messagesCache;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        try {
            messagesCache = new RedisClusterMessagesCache(getRedisCluster());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        getRedisCluster().useWriteCluster(connection -> connection.sync().flushall());
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
}
