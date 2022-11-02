/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.lettuce.core.cluster.SlotHash;
import java.nio.ByteBuffer;
import java.time.Clock;
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
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.tests.util.MessagesDynamoDbExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

class MessagePersisterIntegrationTest {

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = MessagesDynamoDbExtension.build();

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private ExecutorService notificationExecutorService;
  private ExecutorService messageDeletionExecutorService;
  private MessagesCache messagesCache;
  private MessagesManager messagesManager;
  private MessagePersister messagePersister;
  private Account account;

  private static final Duration PERSIST_DELAY = Duration.ofMinutes(10);

  @BeforeEach
  void setUp() throws Exception {
    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection -> {
      connection.sync().flushall();
      connection.sync().upstream().commands().configSet("notify-keyspace-events", "K$glz");
    });

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());

    messageDeletionExecutorService = Executors.newSingleThreadExecutor();
    final MessagesDynamoDb messagesDynamoDb = new MessagesDynamoDb(dynamoDbExtension.getDynamoDbClient(),
        dynamoDbExtension.getDynamoDbAsyncClient(), MessagesDynamoDbExtension.TABLE_NAME, Duration.ofDays(14),
        messageDeletionExecutorService);
    final AccountsManager accountsManager = mock(AccountsManager.class);

    notificationExecutorService = Executors.newSingleThreadExecutor();
    messagesCache = new MessagesCache(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        REDIS_CLUSTER_EXTENSION.getRedisCluster(), Clock.systemUTC(), notificationExecutorService,
        messageDeletionExecutorService);
    messagesManager = new MessagesManager(messagesDynamoDb, messagesCache, mock(ReportMessageManager.class),
        messageDeletionExecutorService);
    messagePersister = new MessagePersister(messagesCache, messagesManager, accountsManager,
        dynamicConfigurationManager, PERSIST_DELAY);

    account = mock(Account.class);

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(accountUuid);
    when(accountsManager.getByAccountIdentifier(accountUuid)).thenReturn(Optional.of(account));

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());

    messagesCache.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    notificationExecutorService.shutdown();
    notificationExecutorService.awaitTermination(15, TimeUnit.SECONDS);

    messageDeletionExecutorService.shutdown();
    messageDeletionExecutorService.awaitTermination(15, TimeUnit.SECONDS);
  }

  @Test
  void testScheduledPersistMessages() {

    final int messageCount = 377;
    final List<MessageProtos.Envelope> expectedMessages = new ArrayList<>(messageCount);
    final Instant now = Instant.now();

    assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {

      for (int i = 0; i < messageCount; i++) {
        final UUID messageGuid = UUID.randomUUID();
        final long timestamp = now.minus(PERSIST_DELAY.multipliedBy(2)).toEpochMilli() + i;

        final MessageProtos.Envelope message = generateRandomMessage(messageGuid, timestamp);

        messagesCache.insert(messageGuid, account.getUuid(), 1, message);
        expectedMessages.add(message);
      }

      REDIS_CLUSTER_EXTENSION.getRedisCluster()
          .useCluster(connection -> connection.sync().set(MessagesCache.NEXT_SLOT_TO_PERSIST_KEY,
              String.valueOf(SlotHash.getSlot(MessagesCache.getMessageQueueKey(account.getUuid(), 1)) - 1)));

      final AtomicBoolean messagesPersisted = new AtomicBoolean(false);

      messagesManager.addMessageAvailabilityListener(account.getUuid(), 1, new MessageAvailabilityListener() {
        @Override
        public boolean handleNewMessagesAvailable() {
          return true;
        }

        @Override
        public boolean handleMessagesPersisted() {
          synchronized (messagesPersisted) {
            messagesPersisted.set(true);
            messagesPersisted.notifyAll();
            return true;
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

      DynamoDbClient dynamoDB = dynamoDbExtension.getDynamoDbClient();

      final List<MessageProtos.Envelope> persistedMessages =
          dynamoDB.scan(ScanRequest.builder().tableName(MessagesDynamoDbExtension.TABLE_NAME).build()).items().stream()
              .map(item -> {
                try {
                  return MessagesDynamoDb.convertItemToEnvelope(item);
                } catch (InvalidProtocolBufferException e) {
                  fail("Could not parse stored message", e);
                  return null;
                }
              })
              .toList();

      assertEquals(expectedMessages, persistedMessages);
    });
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
        .setDestinationUuid(UUID.randomUUID().toString())
        .build();
  }
}
