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
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.WebSocketConnectionEventListener;
import org.whispersystems.textsecuregcm.push.WebSocketConnectionEventManager;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

class MessagePersisterIntegrationTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.MESSAGES);

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private Scheduler messageDeliveryScheduler;
  private ExecutorService messageDeletionExecutorService;
  private ExecutorService websocketConnectionEventExecutor;
  private ExecutorService asyncOperationQueueingExecutor;
  private MessagesCache messagesCache;
  private MessagesManager messagesManager;
  private WebSocketConnectionEventManager webSocketConnectionEventManager;
  private MessagePersister messagePersister;
  private Account account;

  private static final Duration PERSIST_DELAY = Duration.ofMinutes(10);

  @BeforeEach
  void setUp() throws Exception {
    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection -> {
      connection.sync().flushall();
    });

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());

    messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");
    messageDeletionExecutorService = Executors.newSingleThreadExecutor();
    final MessagesDynamoDb messagesDynamoDb = new MessagesDynamoDb(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(), Tables.MESSAGES.tableName(), Duration.ofDays(14),
        messageDeletionExecutorService);
    final AccountsManager accountsManager = mock(AccountsManager.class);

    messagesCache = new MessagesCache(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        messageDeliveryScheduler, messageDeletionExecutorService, Clock.systemUTC());
    messagesManager = new MessagesManager(messagesDynamoDb, messagesCache, mock(ReportMessageManager.class),
        messageDeletionExecutorService, Clock.systemUTC());

    websocketConnectionEventExecutor = Executors.newVirtualThreadPerTaskExecutor();
    asyncOperationQueueingExecutor = Executors.newSingleThreadExecutor();
    webSocketConnectionEventManager = new WebSocketConnectionEventManager(mock(AccountsManager.class),
        mock(PushNotificationManager.class),
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        websocketConnectionEventExecutor,
        asyncOperationQueueingExecutor);

    webSocketConnectionEventManager.start();

    messagePersister = new MessagePersister(messagesCache, messagesManager, accountsManager,
        dynamicConfigurationManager, mock(ExperimentEnrollmentManager.class), mock(DisconnectionRequestManager.class),
        PERSIST_DELAY, 1);

    account = mock(Account.class);

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(accountUuid);
    when(accountsManager.getByAccountIdentifier(accountUuid)).thenReturn(Optional.of(account));
    when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(DevicesHelper.createDevice(Device.PRIMARY_ID)));

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @AfterEach
  void tearDown() throws Exception {
    messageDeletionExecutorService.shutdown();
    messageDeletionExecutorService.awaitTermination(15, TimeUnit.SECONDS);

    websocketConnectionEventExecutor.shutdown();
    websocketConnectionEventExecutor.awaitTermination(15, TimeUnit.SECONDS);

    asyncOperationQueueingExecutor.shutdown();
    asyncOperationQueueingExecutor.awaitTermination(15, TimeUnit.SECONDS);

    messageDeliveryScheduler.dispose();

    webSocketConnectionEventManager.stop();
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

        messagesCache.insert(messageGuid, account.getUuid(), Device.PRIMARY_ID, message).join();
        expectedMessages.add(message);
      }

      REDIS_CLUSTER_EXTENSION.getRedisCluster()
          .useCluster(connection -> connection.sync().set(MessagesCache.NEXT_SLOT_TO_PERSIST_KEY,
              String.valueOf(
                  SlotHash.getSlot(MessagesCache.getMessageQueueKey(account.getUuid(), Device.PRIMARY_ID)) - 1)));

      final AtomicBoolean messagesPersisted = new AtomicBoolean(false);

      webSocketConnectionEventManager.handleClientConnected(account.getUuid(), Device.PRIMARY_ID, new WebSocketConnectionEventListener() {
        @Override
        public void handleNewMessageAvailable() {
        }

        @Override
        public void handleMessagesPersisted() {
          synchronized (messagesPersisted) {
            messagesPersisted.set(true);
            messagesPersisted.notifyAll();
          }
        }

        @Override
        public void handleConnectionDisplaced(final boolean connectedElsewhere) {
        }
      });

      messagePersister.start();

      synchronized (messagesPersisted) {
        while (!messagesPersisted.get()) {
          messagesPersisted.wait();
        }
      }

      messagePersister.stop();

      DynamoDbClient dynamoDB = DYNAMO_DB_EXTENSION.getDynamoDbClient();

      final List<MessageProtos.Envelope> persistedMessages =
          dynamoDB.scan(ScanRequest.builder().tableName(Tables.MESSAGES.tableName()).build()).items().stream()
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

  private MessageProtos.Envelope generateRandomMessage(final UUID messageGuid, final long serverTimestamp) {
    return MessageProtos.Envelope.newBuilder()
        .setClientTimestamp(serverTimestamp * 2) // client timestamp may not be accurate
        .setServerTimestamp(serverTimestamp)
        .setContent(ByteString.copyFromUtf8(RandomStringUtils.secure().nextAlphanumeric(256)))
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
        .setServerGuid(messageGuid.toString())
        .setDestinationServiceId(UUID.randomUUID().toString())
        .build();
  }
}
