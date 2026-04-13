/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.Tags;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicMessagePersisterConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.push.MessageAvailabilityListener;
import org.whispersystems.textsecuregcm.push.RedisMessageAvailabilityManager;
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
  private Scheduler persistQueueScheduler;
  private ExecutorService messageDeletionExecutorService;
  private ExecutorService websocketConnectionEventExecutor;
  private ExecutorService asyncOperationQueueingExecutor;
  private MessagesCache messagesCache;
  private RedisMessageAvailabilityManager redisMessageAvailabilityManager;
  private MessagePersister messagePersister;
  private Account account;
  private ExperimentEnrollmentManager experimentEnrollmentManager;

  private static final Duration PERSIST_DELAY = Duration.ofMinutes(10);

  @BeforeEach
  void setUp() throws Exception {
    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection -> connection.sync().flushall());

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);

    when(dynamicConfiguration.getMessagePersisterConfiguration())
        .thenReturn(new DynamicMessagePersisterConfiguration(true, 1.5, Duration.ofMinutes(5), Duration.ZERO));

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");
    persistQueueScheduler = Schedulers.newBoundedElastic(10, 10_000, "persistQueue");
    messageDeletionExecutorService = Executors.newSingleThreadExecutor();
    experimentEnrollmentManager = mock(ExperimentEnrollmentManager.class);
    final MessagesDynamoDb messagesDynamoDb = new MessagesDynamoDb(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(), Tables.MESSAGES.tableName(), Duration.ofDays(14),
        messageDeletionExecutorService, experimentEnrollmentManager);
    final AccountsManager accountsManager = mock(AccountsManager.class);

    messagesCache = new MessagesCache(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        messageDeliveryScheduler, messageDeletionExecutorService, mock(ScheduledExecutorService.class), Clock.systemUTC(), experimentEnrollmentManager);

    final MessagesManager messagesManager = new MessagesManager(messagesDynamoDb,
        messagesCache,
        mock(RedisMessageAvailabilityManager.class),
        mock(ReportMessageManager.class),
        messageDeletionExecutorService,
        Clock.systemUTC());

    websocketConnectionEventExecutor = Executors.newVirtualThreadPerTaskExecutor();
    asyncOperationQueueingExecutor = Executors.newSingleThreadExecutor();
    redisMessageAvailabilityManager = new RedisMessageAvailabilityManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        websocketConnectionEventExecutor,
        asyncOperationQueueingExecutor);

    redisMessageAvailabilityManager.start();

    messagePersister = new MessagePersister(messagesCache,
        messagesManager,
        accountsManager,
        dynamicConfigurationManager,
        persistQueueScheduler,
        Clock.systemUTC(),
        PERSIST_DELAY,
        1,
        1024);

    account = mock(Account.class);

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(accountUuid);
    when(accountsManager.getByAccountIdentifier(accountUuid)).thenReturn(Optional.of(account));
    when(accountsManager.getByAccountIdentifierAsync(accountUuid)).thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
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
    persistQueueScheduler.dispose();

    redisMessageAvailabilityManager.stop();
  }

  @Test
  void testPersistMessages() throws InterruptedException {

    final int messageCount = 377;
    final List<MessageProtos.Envelope> expectedMessages = new ArrayList<>(messageCount);
    final Instant now = Instant.now();

    for (int i = 0; i < messageCount; i++) {
      final UUID messageGuid = UUID.randomUUID();
      final long timestamp = now.minus(PERSIST_DELAY.multipliedBy(2)).toEpochMilli() + i;

      final MessageProtos.Envelope message = generateRandomMessage(messageGuid, timestamp, false);

      messagesCache.insert(messageGuid, account.getUuid(), Device.PRIMARY_ID, message).join();
      expectedMessages.add(message);
    }

    final CountDownLatch messagesPersistedLatch = new CountDownLatch(1);

    redisMessageAvailabilityManager.handleClientConnected(account.getUuid(), Device.PRIMARY_ID,
            new MessageAvailabilityListener() {
              @Override
              public void handleNewMessageAvailable() {
              }

              @Override
              public void handleMessagesPersisted() {
                messagesPersistedLatch.countDown();
              }

              @Override
              public void handleConflictingMessageConsumer() {
              }
            })
        .toCompletableFuture()
        .join();

    messagePersister.start();

    assertTrue(messagesPersistedLatch.await(15, TimeUnit.SECONDS));

    messagePersister.stop();

    final DynamoDbClient dynamoDB = DYNAMO_DB_EXTENSION.getDynamoDbClient();

    final List<MessageProtos.Envelope> persistedMessages =
        dynamoDB.scan(ScanRequest.builder().tableName(Tables.MESSAGES.tableName()).build()).items().stream()
            .map(item -> {
              try {
                return MessagesDynamoDb.convertItemToEnvelope(item, experimentEnrollmentManager);
              } catch (InvalidProtocolBufferException e) {
                fail("Could not parse stored message", e);
                return null;
              }
            })
            .toList();

    assertEquals(expectedMessages, persistedMessages);
  }

  @Test
  void testPersistFirstPageDiscarded() {
    final int discardableMessages = MessagePersister.MESSAGE_BATCH_LIMIT * 2;
    final int persistableMessages = MessagePersister.MESSAGE_BATCH_LIMIT + 1;

    final Instant now = Instant.now();

    for (int i = 0; i < discardableMessages; i++) {
      final UUID messageGuid = UUID.randomUUID();
      final long timestamp = now.minus(PERSIST_DELAY.multipliedBy(2)).toEpochMilli() + i;

      final MessageProtos.Envelope message = generateRandomMessage(messageGuid, timestamp, true);

      messagesCache.insert(messageGuid, account.getUuid(), Device.PRIMARY_ID, message).join();
    }

    final List<MessageProtos.Envelope> expectedMessages = new ArrayList<>(persistableMessages);

    for (int i = 0; i < persistableMessages; i++) {
      final UUID messageGuid = UUID.randomUUID();
      final long timestamp = now.minus(PERSIST_DELAY.multipliedBy(2)).toEpochMilli() + i;

      final MessageProtos.Envelope message = generateRandomMessage(messageGuid, timestamp, false);

      messagesCache.insert(messageGuid, account.getUuid(), Device.PRIMARY_ID, message).join();
      expectedMessages.add(message);
    }

    messagePersister.persistQueue(account, account.getDevice(Device.PRIMARY_ID).orElseThrow(), Tags.empty()).block();

    final DynamoDbClient dynamoDB = DYNAMO_DB_EXTENSION.getDynamoDbClient();

    final List<MessageProtos.Envelope> persistedMessages =
        dynamoDB.scan(ScanRequest.builder().tableName(Tables.MESSAGES.tableName()).build()).items().stream()
            .map(item -> {
              try {
                return MessagesDynamoDb.convertItemToEnvelope(item, experimentEnrollmentManager);
              } catch (InvalidProtocolBufferException e) {
                fail("Could not parse stored message", e);
                return null;
              }
            })
            .toList();

    assertEquals(expectedMessages, persistedMessages);
    assertFalse(messagesCache.hasMessagesAsync(account.getUuid(), Device.PRIMARY_ID).join());
  }

  private MessageProtos.Envelope generateRandomMessage(final UUID messageGuid, final long serverTimestamp, final boolean ephemeral) {
    return MessageProtos.Envelope.newBuilder()
        .setClientTimestamp(serverTimestamp * 2) // client timestamp may not be accurate
        .setServerTimestamp(serverTimestamp)
        .setContent(ByteString.copyFromUtf8(RandomStringUtils.secure().nextAlphanumeric(256)))
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
        .setServerGuid(messageGuid.toString())
        .setDestinationServiceId(UUID.randomUUID().toString())
        .setEphemeral(ephemeral)
        .build();
  }
}
