package org.whispersystems.textsecuregcm.workers;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.redis.AbstractRedisSingletonTest;
import org.whispersystems.textsecuregcm.storage.Messages;
import org.whispersystems.textsecuregcm.storage.MessagesCache;

import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ScourMessageCacheCommandTest extends AbstractRedisSingletonTest {

    private Messages                 messageDatabase;
    private MessagesCache            messagesCache;
    private ScourMessageCacheCommand scourMessageCacheCommand;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        messageDatabase          = mock(Messages.class);
        messagesCache            = new MessagesCache(getJedisPool());
        scourMessageCacheCommand = new ScourMessageCacheCommand();

        scourMessageCacheCommand.setMessageDatabase(messageDatabase);
        scourMessageCacheCommand.setRedisClient(getRedisClient());
    }

    @Test
    public void testScourMessageCache() {
        final int messageCount = insertDetachedMessages(100, 1_000);

        scourMessageCacheCommand.scourMessageCache();

        verify(messageDatabase, times(messageCount)).store(any(UUID.class), any(MessageProtos.Envelope.class), anyString(), anyLong());
        assertEquals(0, (long)getRedisClient().withClient(connection -> connection.sync().dbsize()));
    }

    @SuppressWarnings("SameParameterValue")
    private int insertDetachedMessages(final int accounts, final int maxMessagesPerAccount) {
        int totalMessages = 0;

        final Random random = new Random();

        for (int i = 0; i < accounts; i++) {
            final String accountNumber = String.format("+1800%07d", i);
            final UUID accountUuid     = UUID.randomUUID();
            final int messageCount     = random.nextInt(maxMessagesPerAccount);

            for (int j = 0; j < messageCount; j++) {
                final UUID messageGuid = UUID.randomUUID();

                final MessageProtos.Envelope envelope = MessageProtos.Envelope.newBuilder()
                        .setTimestamp(System.currentTimeMillis())
                        .setServerTimestamp(System.currentTimeMillis())
                        .setContent(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(256)))
                        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
                        .setServerGuid(messageGuid.toString())
                        .build();

                messagesCache.insert(messageGuid, accountNumber, accountUuid, 1, envelope);
            }

            totalMessages += messageCount;
        }

        getRedisClient().useClient(connection -> connection.sync().del("user_queue_index"));

        return totalMessages;
    }
}
