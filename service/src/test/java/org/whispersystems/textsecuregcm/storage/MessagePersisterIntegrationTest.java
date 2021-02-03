/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.google.protobuf.ByteString;
import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import io.lettuce.core.cluster.SlotHash;
import org.apache.commons.lang3.RandomStringUtils;
import org.jdbi.v3.core.Jdbi;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;
import org.whispersystems.textsecuregcm.tests.util.MessagesDynamoDbRule;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MessagePersisterIntegrationTest extends AbstractRedisClusterTest {

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("messagedb.xml"));

    @Rule
    public MessagesDynamoDbRule messagesDynamoDbRule = new MessagesDynamoDbRule();

    private ExecutorService  notificationExecutorService;
    private MessagesCache    messagesCache;
    private MessagesManager  messagesManager;
    private MessagePersister messagePersister;
    private Account          account;
    private ExperimentEnrollmentManager experimentEnrollmentManager;

    private static final Duration PERSIST_DELAY = Duration.ofMinutes(10);

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        getRedisCluster().useCluster(connection -> {
            connection.sync().flushall();
            connection.sync().masters().commands().configSet("notify-keyspace-events", "K$glz");
        });

        final Messages        messages        = new Messages(new FaultTolerantDatabase("messages-test", Jdbi.create(db.getTestDatabase()), new CircuitBreakerConfiguration()));
        final MessagesDynamoDb messagesDynamoDb = new MessagesDynamoDb(messagesDynamoDbRule.getDynamoDB(), MessagesDynamoDbRule.TABLE_NAME, Duration.ofDays(7));
        final AccountsManager accountsManager = mock(AccountsManager.class);

        experimentEnrollmentManager = mock(ExperimentEnrollmentManager.class);
        when(experimentEnrollmentManager.isEnrolled(any(UUID.class), anyString())).thenReturn(Boolean.TRUE);

        notificationExecutorService = Executors.newSingleThreadExecutor();
        messagesCache               = new MessagesCache(getRedisCluster(), getRedisCluster(), notificationExecutorService);
        messagesManager             = new MessagesManager(messages, messagesDynamoDb, messagesCache, mock(PushLatencyManager.class), experimentEnrollmentManager);
        messagePersister            = new MessagePersister(messagesCache, messagesManager, accountsManager, mock(FeatureFlagsManager.class), PERSIST_DELAY);

        account = mock(Account.class);

        final UUID accountUuid = UUID.randomUUID();

        when(account.getNumber()).thenReturn("+18005551234");
        when(account.getUuid()).thenReturn(accountUuid);
        when(accountsManager.get(accountUuid)).thenReturn(Optional.of(account));

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

        DynamoDB dynamoDB = messagesDynamoDbRule.getDynamoDB();
        Table table = dynamoDB.getTable(MessagesDynamoDbRule.TABLE_NAME);
        final ItemCollection<ScanOutcome> scan = table.scan(new ScanSpec());
        for (Item item : scan) {
            persistedMessages.add(MessageProtos.Envelope.newBuilder()
                                                        .setServerGuid(convertBinaryToUuid(item.getBinary("U")).toString())
                                                        .setType(MessageProtos.Envelope.Type.valueOf(item.getInt("T")))
                                                        .setTimestamp(item.getLong("TS"))
                                                        .setServerTimestamp(extractServerTimestamp(item.getBinary("S")))
                                                        .setContent(ByteString.copyFrom(item.getBinary("C")))
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
