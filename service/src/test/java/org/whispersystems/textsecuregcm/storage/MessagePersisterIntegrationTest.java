/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.lettuce.core.cluster.SlotHash;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;
import org.whispersystems.textsecuregcm.tests.util.MessagesDynamoDbRule;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

public class MessagePersisterIntegrationTest extends AbstractRedisClusterTest {

    @Rule
    public MessagesDynamoDbRule messagesDynamoDbRule = new MessagesDynamoDbRule();

    private ExecutorService  notificationExecutorService;
    private MessagesCache    messagesCache;
    private MessagesManager  messagesManager;
    private MessagePersister messagePersister;
    private Account          account;

    private static final Duration PERSIST_DELAY = Duration.ofMinutes(10);

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        getRedisCluster().useCluster(connection -> {
            connection.sync().flushall();
            connection.sync().upstream().commands().configSet("notify-keyspace-events", "K$glz");
        });

        final MessagesDynamoDb messagesDynamoDb = new MessagesDynamoDb(messagesDynamoDbRule.getDynamoDbClient(), MessagesDynamoDbRule.TABLE_NAME, Duration.ofDays(7));
        final AccountsManager accountsManager = mock(AccountsManager.class);
        final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);

        notificationExecutorService = Executors.newSingleThreadExecutor();
        messagesCache               = new MessagesCache(getRedisCluster(), getRedisCluster(), notificationExecutorService);
        messagesManager             = new MessagesManager(messagesDynamoDb, messagesCache, mock(PushLatencyManager.class), mock(ReportMessageManager.class));
        messagePersister            = new MessagePersister(messagesCache, messagesManager, accountsManager, dynamicConfigurationManager, PERSIST_DELAY);

        account = mock(Account.class);

        final UUID accountUuid = UUID.randomUUID();

        when(account.getNumber()).thenReturn("+18005551234");
        when(account.getUuid()).thenReturn(accountUuid);
        when(accountsManager.get(accountUuid)).thenReturn(Optional.of(account));
        when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());

        messagesCache.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        notificationExecutorService.shutdown();
        notificationExecutorService.awaitTermination(15, TimeUnit.SECONDS);
    }

    @Test(timeout = 15_000)
    public void testScheduledPersistMessages() throws Exception {
        final int                          messageCount     = 377;
        final List<MessageProtos.Envelope> expectedMessages = new ArrayList<>(messageCount);
        final Instant                      now              = Instant.now();

        for (int i = 0; i < messageCount; i++) {
            final UUID messageGuid = UUID.randomUUID();
            final long timestamp   = now.minus(PERSIST_DELAY.multipliedBy(2)).toEpochMilli() + i;

            final MessageProtos.Envelope message = generateRandomMessage(messageGuid, timestamp);

            messagesCache.insert(messageGuid, account.getUuid(), 1, message);
            expectedMessages.add(message);
        }

        getRedisCluster().useCluster(connection -> connection.sync().set(MessagesCache.NEXT_SLOT_TO_PERSIST_KEY, String.valueOf(SlotHash.getSlot(MessagesCache.getMessageQueueKey(account.getUuid(), 1)) - 1)));

        final AtomicBoolean messagesPersisted = new AtomicBoolean(false);

        messagesManager.addMessageAvailabilityListener(account.getUuid(), 1, new MessageAvailabilityListener() {
            @Override
            public void handleNewMessagesAvailable() {
            }

            @Override
            public void handleNewEphemeralMessageAvailable() {
            }

            @Override
            public void handleMessagesPersisted() {
                synchronized (messagesPersisted) {
                    messagesPersisted.set(true);
                    messagesPersisted.notifyAll();
                }
            }
        });

        messagePersister.start();

        synchronized (messagesPersisted) {
            while (!messagesPersisted.get()) {
                messagesPersisted.wait();
            }
        }

        messagePersister.stop();

        final List<MessageProtos.Envelope> persistedMessages = new ArrayList<>(messageCount);

        DynamoDbClient dynamoDB = messagesDynamoDbRule.getDynamoDbClient();
      for (Map<String, AttributeValue> item : dynamoDB
          .scan(ScanRequest.builder().tableName(MessagesDynamoDbRule.TABLE_NAME).build()).items()) {
        persistedMessages.add(MessageProtos.Envelope.newBuilder()
            .setServerGuid(AttributeValues.getUUID(item, "U", null).toString())
            .setType(MessageProtos.Envelope.Type.valueOf(AttributeValues.getInt(item, "T", -1)))
            .setTimestamp(AttributeValues.getLong(item, "TS", -1))
            .setServerTimestamp(extractServerTimestamp(AttributeValues.getByteArray(item, "S", null)))
            .setContent(ByteString.copyFrom(AttributeValues.getByteArray(item, "C", null)))
            .build());
        }

        assertEquals(expectedMessages, persistedMessages);
    }

    private static UUID convertBinaryToUuid(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long msb = bb.getLong();
        long lsb = bb.getLong();
        return new UUID(msb, lsb);
    }

    private static long extractServerTimestamp(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.getLong();
        return bb.getLong();
    }

    private MessageProtos.Envelope generateRandomMessage(final UUID messageGuid, final long timestamp) {
        return MessageProtos.Envelope.newBuilder()
                .setTimestamp(timestamp)
                .setServerTimestamp(timestamp)
                .setContent(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(256)))
                .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
                .setServerGuid(messageGuid.toString())
                .build();
    }
}
