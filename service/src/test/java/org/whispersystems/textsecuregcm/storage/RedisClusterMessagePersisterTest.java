package org.whispersystems.textsecuregcm.storage;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.push.PushSender;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RedisClusterMessagePersisterTest {

    private RedisClusterMessagesCache    messagesCache;
    private Messages                     messagesDatabase;
    private RedisClusterMessagePersister messagePersister;
    private AccountsManager              accountsManager;

    private long serialTimestamp = 0;

    private static final UUID   DESTINATION_ACCOUNT_UUID   = UUID.randomUUID();
    private static final String DESTINATION_ACCOUNT_NUMBER = "+18005551234";
    private static final long   DESTINATION_DEVICE_ID      = 7;

    private static final Duration PERSIST_DELAY = Duration.ofMinutes(5);

    private static final Random RANDOM = new Random();

    @Before
    public void setUp() {
        messagesCache    = mock(RedisClusterMessagesCache.class);
        messagesDatabase = mock(Messages.class);
        accountsManager  = mock(AccountsManager.class);

        final Account account = mock(Account.class);

        when(accountsManager.get(DESTINATION_ACCOUNT_UUID)).thenReturn(Optional.of(account));
        when(account.getNumber()).thenReturn(DESTINATION_ACCOUNT_NUMBER);

        messagePersister = new RedisClusterMessagePersister(messagesCache, messagesDatabase, mock(PubSubManager.class), mock(PushSender.class), accountsManager, PERSIST_DELAY);
    }

    @Test
    public void testPersistNextQueuesNoQueues() {
        final int slot = 7;

        when(messagesCache.getNextSlotToPersist()).thenReturn(slot);
        when(messagesCache.getQueuesToPersist(eq(slot), any(Instant.class), eq(RedisClusterMessagePersister.QUEUE_BATCH_LIMIT))).thenReturn(Collections.emptyList());

        messagePersister.persistNextQueues(Instant.now());

        verify(messagesCache, never()).lockQueueForPersistence(any());
    }

    @Test
    public void testPersistNextQueuesSingleQueue() {
        final int slot = 7;
        final String queueName = new String(RedisClusterMessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);

        when(messagesCache.getNextSlotToPersist()).thenReturn(slot);
        when(messagesCache.getQueuesToPersist(eq(slot), any(Instant.class), eq(RedisClusterMessagePersister.QUEUE_BATCH_LIMIT))).thenReturn(List.of(queueName));

        messagePersister.persistNextQueues(Instant.now());

        verify(messagesCache).lockQueueForPersistence(queueName);
    }

    @Test
    public void testPersistNextQueuesMultiplePages() {
        final int slot = 7;
        final int queueCount = RedisClusterMessagePersister.QUEUE_BATCH_LIMIT * 3;

        final List<String> queues = new ArrayList<>(queueCount);

        for (int i = 0; i < queueCount; i++) {
            final String queueName = generateRandomQueueName();
            final UUID accountUuid = RedisClusterMessagesCache.getAccountUuidFromQueueName(queueName);

            queues.add(queueName);

            final Account account = mock(Account.class);

            when(accountsManager.get(accountUuid)).thenReturn(Optional.of(account));
            when(account.getNumber()).thenReturn("+1" + RandomStringUtils.randomNumeric(10));
        }

        when(messagesCache.getNextSlotToPersist()).thenReturn(slot);
        when(messagesCache.getQueuesToPersist(eq(slot), any(Instant.class), eq(RedisClusterMessagePersister.QUEUE_BATCH_LIMIT)))
                .thenReturn(queues.subList(0,                                                  RedisClusterMessagePersister.QUEUE_BATCH_LIMIT))
                .thenReturn(queues.subList(RedisClusterMessagePersister.QUEUE_BATCH_LIMIT,     RedisClusterMessagePersister.QUEUE_BATCH_LIMIT * 2))
                .thenReturn(queues.subList(RedisClusterMessagePersister.QUEUE_BATCH_LIMIT * 2, RedisClusterMessagePersister.QUEUE_BATCH_LIMIT * 3))
                .thenReturn(Collections.emptyList());

        messagePersister.persistNextQueues(Instant.now());

        verify(messagesCache, times(queueCount)).lockQueueForPersistence(any());
    }

    @Test
    @Ignore
    public void testPersistQueueNoMessages() {
        final String queueName = new String(RedisClusterMessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);

        when(messagesCache.getMessagesToPersist(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, RedisClusterMessagePersister.MESSAGE_BATCH_LIMIT)).thenReturn(Collections.emptyList());

        messagePersister.persistQueue(queueName);

        verify(messagesCache).lockQueueForPersistence(queueName);
        verify(messagesCache).getMessagesToPersist(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, RedisClusterMessagePersister.MESSAGE_BATCH_LIMIT);
        verify(messagesDatabase, never()).store(any(), any(), any(), anyLong());
        verify(messagesCache, never()).remove(anyString(), any(UUID.class), anyLong(), any(UUID.class));
        verify(messagesCache).unlockQueueForPersistence(queueName);
    }

    @Test
    @Ignore
    public void testPersistQueueSingleMessage() {
        final String queueName = new String(RedisClusterMessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);

        final MessageProtos.Envelope message = generateRandomMessage();

        when(messagesCache.getMessagesToPersist(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, RedisClusterMessagePersister.MESSAGE_BATCH_LIMIT)).thenReturn(List.of(message));

        messagePersister.persistQueue(queueName);

        verify(messagesCache).lockQueueForPersistence(queueName);
        verify(messagesCache).getMessagesToPersist(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, RedisClusterMessagePersister.MESSAGE_BATCH_LIMIT);
        verify(messagesDatabase).store(UUID.fromString(message.getServerGuid()), message, DESTINATION_ACCOUNT_NUMBER, DESTINATION_DEVICE_ID);
        verify(messagesCache).remove(DESTINATION_ACCOUNT_NUMBER, DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, UUID.fromString(message.getServerGuid()));
        verify(messagesCache).unlockQueueForPersistence(queueName);
    }

    @Test
    @Ignore
    public void testPersistQueueMultiplePages() {
        final int messageCount = RedisClusterMessagePersister.MESSAGE_BATCH_LIMIT * 3;
        final List<MessageProtos.Envelope> messagesInQueue = new ArrayList<>(messageCount);

        for (int i = 0; i < messageCount; i++) {
            messagesInQueue.add(generateRandomMessage());
        }

        when(messagesCache.getMessagesToPersist(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID,          RedisClusterMessagePersister.MESSAGE_BATCH_LIMIT))
                .thenReturn(messagesInQueue.subList(0,                                                    RedisClusterMessagePersister.MESSAGE_BATCH_LIMIT))
                .thenReturn(messagesInQueue.subList(RedisClusterMessagePersister.MESSAGE_BATCH_LIMIT,     RedisClusterMessagePersister.MESSAGE_BATCH_LIMIT * 2))
                .thenReturn(messagesInQueue.subList(RedisClusterMessagePersister.MESSAGE_BATCH_LIMIT * 2, RedisClusterMessagePersister.MESSAGE_BATCH_LIMIT * 3))
                .thenReturn(Collections.emptyList());

        final String queueName = new String(RedisClusterMessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);

        messagePersister.persistQueue(queueName);

        verify(messagesCache).lockQueueForPersistence(queueName);
        verify(messagesCache, times(4)).getMessagesToPersist(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, RedisClusterMessagePersister.MESSAGE_BATCH_LIMIT);
        verify(messagesDatabase, times(messageCount)).store(any(UUID.class), any(MessageProtos.Envelope.class), eq(DESTINATION_ACCOUNT_NUMBER), eq(DESTINATION_DEVICE_ID));
        verify(messagesCache, times(messageCount)).remove(eq(DESTINATION_ACCOUNT_NUMBER), eq(DESTINATION_ACCOUNT_UUID), eq(DESTINATION_DEVICE_ID), any(UUID.class));
        verify(messagesCache).unlockQueueForPersistence(queueName);
    }

    private MessageProtos.Envelope generateRandomMessage() {
        final MessageProtos.Envelope.Builder envelopeBuilder = MessageProtos.Envelope.newBuilder()
                .setTimestamp(serialTimestamp++)
                .setServerTimestamp(serialTimestamp++)
                .setContent(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(256)))
                .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
                .setServerGuid(UUID.randomUUID().toString());

        return envelopeBuilder.build();
    }

    private String generateRandomQueueName() {
        return String.format("user_queue::{%s::%d}", UUID.randomUUID().toString(), RANDOM.nextInt(10));
    }
}
