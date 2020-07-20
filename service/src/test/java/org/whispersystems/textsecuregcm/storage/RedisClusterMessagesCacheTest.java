package org.whispersystems.textsecuregcm.storage;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

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
}
