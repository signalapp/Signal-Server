/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.protobuf.ByteString;
import io.lettuce.core.cluster.SlotHash;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MessagePersisterTest extends AbstractRedisClusterTest {

    private ExecutorService          notificationExecutorService;
    private MessagesCache            messagesCache;
    private Messages                 messagesDatabase;
    private MessagePersister         messagePersister;
    private AccountsManager          accountsManager;

    private static final UUID   DESTINATION_ACCOUNT_UUID   = UUID.randomUUID();
    private static final String DESTINATION_ACCOUNT_NUMBER = "+18005551234";
    private static final long   DESTINATION_DEVICE_ID      = 7;

    private static final Duration PERSIST_DELAY = Duration.ofMinutes(5);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        final MessagesManager messagesManager         = mock(MessagesManager.class);

        messagesDatabase = mock(Messages.class);
        accountsManager  = mock(AccountsManager.class);

        final Account account = mock(Account.class);

        when(accountsManager.get(DESTINATION_ACCOUNT_UUID)).thenReturn(Optional.of(account));
        when(account.getNumber()).thenReturn(DESTINATION_ACCOUNT_NUMBER);

        notificationExecutorService = Executors.newSingleThreadExecutor();
        messagesCache               = new MessagesCache(getRedisCluster(), notificationExecutorService);
        messagePersister            = new MessagePersister(messagesCache, messagesManager, accountsManager, PERSIST_DELAY);

        doAnswer(invocation -> {
            final String destination                    = invocation.getArgument(0, String.class);
            final UUID destinationUuid                  = invocation.getArgument(1, UUID.class);
            final long deviceId                         = invocation.getArgument(2, Long.class);
            final List<MessageProtos.Envelope> messages = invocation.getArgument(3, List.class);

            messagesDatabase.store(messages, destination, deviceId);

            for (final MessageProtos.Envelope message : messages) {
                messagesCache.remove(destinationUuid, deviceId, UUID.fromString(message.getServerGuid()));
            }

            return null;
        }).when(messagesManager).persistMessages(anyString(), any(UUID.class), anyLong(), any());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        notificationExecutorService.shutdown();
        notificationExecutorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void testPersistNextQueuesNoQueues() {
        messagePersister.persistNextQueues(Instant.now());

        verify(accountsManager, never()).get(any(UUID.class));
    }

    @Test
    public void testPersistNextQueuesSingleQueue() {
        final String  queueName    = new String(MessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);
        final int     messageCount = (MessagePersister.MESSAGE_BATCH_LIMIT * 3) + 7;
        final Instant now          = Instant.now();

        insertMessages(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, messageCount, now);
        setNextSlotToPersist(SlotHash.getSlot(queueName));

        messagePersister.persistNextQueues(now.plus(messagePersister.getPersistDelay()));

        final ArgumentCaptor<List<MessageProtos.Envelope>> messagesCaptor = ArgumentCaptor.forClass(List.class);

        verify(messagesDatabase, atLeastOnce()).store(messagesCaptor.capture(), eq(DESTINATION_ACCOUNT_NUMBER), eq(DESTINATION_DEVICE_ID));
        assertEquals(messageCount, messagesCaptor.getAllValues().stream().mapToInt(List::size).sum());
    }

    @Test
    public void testPersistNextQueuesSingleQueueTooSoon() {
        final String  queueName    = new String(MessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);
        final int     messageCount = (MessagePersister.MESSAGE_BATCH_LIMIT * 3) + 7;
        final Instant now          = Instant.now();

        insertMessages(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, messageCount, now);
        setNextSlotToPersist(SlotHash.getSlot(queueName));

        messagePersister.persistNextQueues(now);

        verify(messagesDatabase, never()).store(any(), anyString(), anyLong());
    }

    @Test
    public void testPersistNextQueuesMultiplePages() {
        final int     slot             = 7;
        final int     queueCount       = (MessagePersister.QUEUE_BATCH_LIMIT * 3) + 7;
        final int     messagesPerQueue = 10;
        final Instant now              = Instant.now();

        for (int i = 0; i < queueCount; i++) {
            final String queueName     = generateRandomQueueNameForSlot(slot);
            final UUID accountUuid     = MessagesCache.getAccountUuidFromQueueName(queueName);
            final long deviceId        = MessagesCache.getDeviceIdFromQueueName(queueName);
            final String accountNumber = "+1" + RandomStringUtils.randomNumeric(10);

            final Account account = mock(Account.class);

            when(accountsManager.get(accountUuid)).thenReturn(Optional.of(account));
            when(account.getNumber()).thenReturn(accountNumber);

            insertMessages(accountUuid, deviceId, messagesPerQueue, now);
        }

        setNextSlotToPersist(slot);

        messagePersister.persistNextQueues(now.plus(messagePersister.getPersistDelay()));

        final ArgumentCaptor<List<MessageProtos.Envelope>> messagesCaptor = ArgumentCaptor.forClass(List.class);

        verify(messagesDatabase, atLeastOnce()).store(messagesCaptor.capture(), anyString(), anyLong());
        assertEquals(queueCount * messagesPerQueue, messagesCaptor.getAllValues().stream().mapToInt(List::size).sum());
    }

    @Test
    public void testPersistQueueRetry() {
        final String  queueName    = new String(MessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);
        final int     messageCount = (MessagePersister.MESSAGE_BATCH_LIMIT * 3) + 7;
        final Instant now          = Instant.now();

        insertMessages(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, messageCount, now);
        setNextSlotToPersist(SlotHash.getSlot(queueName));

        doAnswer((Answer<Void>)invocation -> {
            throw new RuntimeException("OH NO.");
        }).when(messagesDatabase).store(any(), eq(DESTINATION_ACCOUNT_NUMBER), eq(DESTINATION_DEVICE_ID));

        messagePersister.persistNextQueues(now.plus(messagePersister.getPersistDelay()));

        assertEquals(List.of(queueName),
                     messagesCache.getQueuesToPersist(SlotHash.getSlot(queueName), Instant.now().plus(messagePersister.getPersistDelay()), 1));
    }

    @SuppressWarnings("SameParameterValue")
    private static String generateRandomQueueNameForSlot(final int slot) {
        final UUID uuid = UUID.randomUUID();

        final String queueNameBase = "user_queue::{" + uuid.toString() + "::";

        for (int deviceId = 0; deviceId < Integer.MAX_VALUE; deviceId++) {
            final String queueName = queueNameBase + deviceId + "}";

            if (SlotHash.getSlot(queueName) == slot) {
                return queueName;
            }
        }

        throw new IllegalStateException("Could not find a queue name for slot " + slot);
    }

    private void insertMessages(final UUID accountUuid, final long deviceId, final int messageCount, final Instant firstMessageTimestamp) {
        for (int i = 0; i < messageCount; i++) {
            final UUID messageGuid = UUID.randomUUID();

            final MessageProtos.Envelope envelope = MessageProtos.Envelope.newBuilder()
                    .setTimestamp(firstMessageTimestamp.toEpochMilli() + i)
                    .setServerTimestamp(firstMessageTimestamp.toEpochMilli() + i)
                    .setContent(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(256)))
                    .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
                    .setServerGuid(messageGuid.toString())
                    .build();

            messagesCache.insert(messageGuid, accountUuid, deviceId, envelope);
        }
    }

    private void setNextSlotToPersist(final int nextSlot) {
        getRedisCluster().useCluster(connection -> connection.sync().set(MessagesCache.NEXT_SLOT_TO_PERSIST_KEY, String.valueOf(nextSlot - 1)));
    }
}
