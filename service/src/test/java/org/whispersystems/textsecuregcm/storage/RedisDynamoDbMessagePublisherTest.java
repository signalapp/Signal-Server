/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.push.RedisMessageAvailabilityManager;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

@Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RedisDynamoDbMessagePublisherTest {

  private MessagesDynamoDb messagesDynamoDb;
  private MessagesCache messagesCache;
  private RedisMessageAvailabilityManager redisMessageAvailabilityManager;

  private static ExecutorService sharedExecutorService;
  private static Scheduler messageDeliveryScheduler;

  private Device destinationDevice;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(DynamoDbExtensionSchema.Tables.MESSAGES);

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private static final AtomicLong SERIAL_TIMESTAMP = new AtomicLong(0);

  private static final ServiceIdentifier DESTINATION_SERVICE_IDENTIFIER = new AciServiceIdentifier(UUID.randomUUID());

  @BeforeAll
  static void setUpBeforeAll() {
    sharedExecutorService = Executors.newVirtualThreadPerTaskExecutor();
    messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");
  }

  @BeforeEach
  void setUp() throws IOException {
    messagesDynamoDb = new MessagesDynamoDb(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.MESSAGES.tableName(),
        Duration.ofDays(14),
        sharedExecutorService,
        mock(ExperimentEnrollmentManager.class));

    messagesCache = new MessagesCache(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        messageDeliveryScheduler, sharedExecutorService, mock(ScheduledExecutorService.class), Clock.systemUTC(), mock(ExperimentEnrollmentManager.class));

    redisMessageAvailabilityManager = mock(RedisMessageAvailabilityManager.class);

    destinationDevice = mock(Device.class);
    when(destinationDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(destinationDevice.getCreated()).thenReturn(System.currentTimeMillis());
  }

  @AfterAll
  static void tearDownAfterAll() {
    sharedExecutorService.shutdown();
    messageDeliveryScheduler.dispose();
  }

  @Test
  void subscribeDispose() {
    final byte deviceId = Device.PRIMARY_ID;

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);
    when(device.getCreated()).thenReturn(System.currentTimeMillis());

    {
      final UUID accountIdentifier = UUID.randomUUID();

      final RedisDynamoDbMessagePublisher _ =
          new RedisDynamoDbMessagePublisher(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, accountIdentifier, device);

      verify(redisMessageAvailabilityManager, never()).handleClientConnected(eq(accountIdentifier), eq(deviceId), any());
      verify(redisMessageAvailabilityManager, never()).handleClientDisconnected(eq(accountIdentifier), eq(deviceId));
    }

    {
      final UUID accountIdentifier = UUID.randomUUID();

      final RedisDynamoDbMessagePublisher messagePublisher =
          new RedisDynamoDbMessagePublisher(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, accountIdentifier, device);

      JdkFlowAdapter.flowPublisherToFlux(messagePublisher).subscribe();

      verify(redisMessageAvailabilityManager).handleClientConnected(eq(accountIdentifier), eq(deviceId), any());
      verify(redisMessageAvailabilityManager, never()).handleClientDisconnected(eq(accountIdentifier), eq(deviceId));
    }

    {
      final UUID accountIdentifier = UUID.randomUUID();

      final RedisDynamoDbMessagePublisher messagePublisher =
          new RedisDynamoDbMessagePublisher(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, accountIdentifier, device);

      final Disposable disposable = JdkFlowAdapter.flowPublisherToFlux(messagePublisher).subscribe();
      disposable.dispose();

      verify(redisMessageAvailabilityManager).handleClientConnected(eq(accountIdentifier), eq(deviceId), any());
      verify(redisMessageAvailabilityManager).handleClientDisconnected(eq(accountIdentifier), eq(deviceId));
    }
  }

  @Test
  void publishMessages() {
    final MessageProtos.Envelope dynamoDbMessage = insertDynamoDbMessage(generateRandomMessage());
    final MessageProtos.Envelope redisMessage = insertRedisMessage(generateRandomMessage());

    final RedisDynamoDbMessagePublisher messagePublisher =
        new RedisDynamoDbMessagePublisher(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice);

    StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(messagePublisher))
        .expectNext(new MessageStreamEntry.Envelope(dynamoDbMessage))
        .expectNext(new MessageStreamEntry.Envelope(redisMessage))
        .expectNext(new MessageStreamEntry.QueueEmpty())
        .verifyTimeout(Duration.ofMillis(500));
  }

  @Test
  void publishMessagesDynamoDbOnly() {
    final MessageProtos.Envelope dynamoDbMessage = insertDynamoDbMessage(generateRandomMessage());

    final RedisDynamoDbMessagePublisher messagePublisher =
        new RedisDynamoDbMessagePublisher(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice);

    StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(messagePublisher))
        .expectNext(new MessageStreamEntry.Envelope(dynamoDbMessage))
        .expectNext(new MessageStreamEntry.QueueEmpty())
        .verifyTimeout(Duration.ofMillis(500));
  }

  @Test
  void publishMessagesRedisOnly() {
    final MessageProtos.Envelope redisMessage = insertRedisMessage(generateRandomMessage());

    final RedisDynamoDbMessagePublisher messagePublisher =
        new RedisDynamoDbMessagePublisher(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice);

    StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(messagePublisher))
        .expectNext(new MessageStreamEntry.Envelope(redisMessage))
        .expectNext(new MessageStreamEntry.QueueEmpty())
        .verifyTimeout(Duration.ofMillis(500));
  }

  @Test
  void publishMessagesTailNewRedisMessages() {
    final MessageProtos.Envelope dynamoDbMessage = insertDynamoDbMessage(generateRandomMessage());
    final MessageProtos.Envelope redisMessage = insertRedisMessage(generateRandomMessage());

    final MessageProtos.Envelope newArrivalRedisMessage = generateRandomMessage();

    final RedisDynamoDbMessagePublisher messagePublisher =
        new RedisDynamoDbMessagePublisher(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice);

    final CountDownLatch queueEmptyCountDownLatch = new CountDownLatch(1);

    Thread.ofVirtual().start(() -> {
      try {
        queueEmptyCountDownLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }

      deleteRedisMessage(redisMessage);
      messagePublisher.handleMessageAcknowledged();

      deleteDynamoDbMessage(dynamoDbMessage);
      messagePublisher.handleMessageAcknowledged();

      insertRedisMessage(newArrivalRedisMessage);
      messagePublisher.handleNewMessageAvailable();
    });

    StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(messagePublisher)
            .doOnNext(entry -> {
              if (entry instanceof MessageStreamEntry.QueueEmpty) {
                queueEmptyCountDownLatch.countDown();
              }
            }))
        .expectNext(new MessageStreamEntry.Envelope(dynamoDbMessage))
        .expectNext(new MessageStreamEntry.Envelope(redisMessage))
        .expectNext(new MessageStreamEntry.QueueEmpty())
        .expectNext(new MessageStreamEntry.Envelope(newArrivalRedisMessage))
        .verifyTimeout(Duration.ofMillis(500));
  }

  @Test
  void publishMessagesTailNewPersistedMessages() {
    final MessageProtos.Envelope dynamoDbMessage = insertDynamoDbMessage(generateRandomMessage());
    final MessageProtos.Envelope redisMessage = insertRedisMessage(generateRandomMessage());

    final MessageProtos.Envelope persistedMessage = generateRandomMessage();

    final RedisDynamoDbMessagePublisher messagePublisher =
        new RedisDynamoDbMessagePublisher(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice);

    final CountDownLatch queueEmptyCountDownLatch = new CountDownLatch(1);

    Thread.ofVirtual().start(() -> {
      try {
        queueEmptyCountDownLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }

      deleteRedisMessage(redisMessage);
      messagePublisher.handleMessageAcknowledged();

      deleteDynamoDbMessage(dynamoDbMessage);
      messagePublisher.handleMessageAcknowledged();

      insertDynamoDbMessage(persistedMessage);
      messagePublisher.handleMessagesPersisted();
    });

    StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(messagePublisher)
            .doOnNext(entry -> {
              if (entry instanceof MessageStreamEntry.QueueEmpty) {
                queueEmptyCountDownLatch.countDown();
              }
            }))
        .expectNext(new MessageStreamEntry.Envelope(dynamoDbMessage))
        .expectNext(new MessageStreamEntry.Envelope(redisMessage))
        .expectNext(new MessageStreamEntry.QueueEmpty())
        .expectNext(new MessageStreamEntry.Envelope(persistedMessage))
        .verifyTimeout(Duration.ofMillis(500));
  }

  @Test
  void publishMessagesWaitForAcknowledgement() {
    final MessageProtos.Envelope dynamoDbMessage = insertDynamoDbMessage(generateRandomMessage());
    final MessageProtos.Envelope redisMessage = insertRedisMessage(generateRandomMessage());

    final MessageProtos.Envelope persistedMessage = generateRandomMessage();

    final RedisDynamoDbMessagePublisher messagePublisher =
        new RedisDynamoDbMessagePublisher(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice);

    final CountDownLatch queueEmptyCountDownLatch = new CountDownLatch(1);

    Thread.ofVirtual().start(() -> {
      try {
        queueEmptyCountDownLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }

      insertDynamoDbMessage(persistedMessage);
      messagePublisher.handleMessagesPersisted();
    });

    StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(messagePublisher)
            .doOnNext(entry -> {
              if (entry instanceof MessageStreamEntry.QueueEmpty) {
                queueEmptyCountDownLatch.countDown();
              }
            }))
        .expectNext(new MessageStreamEntry.Envelope(dynamoDbMessage))
        .expectNext(new MessageStreamEntry.Envelope(redisMessage))
        .expectNext(new MessageStreamEntry.QueueEmpty())
        .verifyTimeout(Duration.ofMillis(500));
  }

  @Test
  void publishMessagesConsumerConflict() {
    final RedisDynamoDbMessagePublisher messagePublisher =
        new RedisDynamoDbMessagePublisher(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice);

    final CountDownLatch countDownLatch = new CountDownLatch(1);

    Thread.ofVirtual().start(() -> {
      try {
        countDownLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }

      messagePublisher.handleConflictingMessageConsumer();
    });

    StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(messagePublisher)
        .doOnSubscribe(_ -> countDownLatch.countDown()))
            .expectError(ConflictingMessageConsumerException.class)
        .verify();

    verify(redisMessageAvailabilityManager, timeout(1_000)).handleClientConnected(DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice.getId(), messagePublisher);
    verify(redisMessageAvailabilityManager, timeout(1_000)).handleClientDisconnected(DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice.getId());
  }

  @ParameterizedTest
  @CsvSource({
          "207, 173",
          "323, 0",
          "0, 221",
  })
  void publishMessagesMultipleRequests(final int persistedMessageCount, final int cachedMessageCount) {
    final List<MessageProtos.Envelope> expectedMessages = new ArrayList<>(persistedMessageCount + cachedMessageCount);

    for (int i = 0; i < persistedMessageCount; i++) {
      expectedMessages.add(insertDynamoDbMessage(generateRandomMessage()));
    }

    for (int i = 0; i < cachedMessageCount; i++) {
      expectedMessages.add(insertRedisMessage(generateRandomMessage()));
    }

    final RedisDynamoDbMessagePublisher messagePublisher =
        new RedisDynamoDbMessagePublisher(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice);

    final List<MessageProtos.Envelope> publishedMessages = new ArrayList<>(expectedMessages.size());

    final CompletableFuture<Void> queueEmptyFuture = new CompletableFuture<>();

    final Disposable disposable = JdkFlowAdapter.flowPublisherToFlux(messagePublisher)
        .limitRate(20)
        .doOnNext(entry -> {
          if (entry instanceof MessageStreamEntry.Envelope(final MessageProtos.Envelope message)) {
            publishedMessages.add(message);
          } else if (entry instanceof MessageStreamEntry.QueueEmpty) {
            queueEmptyFuture.complete(null);
          }
        })
        .subscribe();

    queueEmptyFuture.thenRun(disposable::dispose).join();

    assertEquals(expectedMessages, publishedMessages);
  }

  @Test
  void publishQueueEmptySignalDeferred() {
    final MessageProtos.Envelope redisMessage = insertRedisMessage(generateRandomMessage());

    {
      final RedisDynamoDbMessagePublisher messagePublisher =
          new RedisDynamoDbMessagePublisher(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager,
              DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice);

      StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(messagePublisher), 1)
          .expectNext(new MessageStreamEntry.Envelope(redisMessage))
          .verifyTimeout(Duration.ofMillis(500));
    }

    {
      final RedisDynamoDbMessagePublisher messagePublisher =
          new RedisDynamoDbMessagePublisher(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager,
              DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice);

      StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(messagePublisher), 2)
          .expectNext(new MessageStreamEntry.Envelope(redisMessage))
          .expectNext(new MessageStreamEntry.QueueEmpty())
          .verifyTimeout(Duration.ofMillis(500));
    }
  }

  private MessageProtos.Envelope insertRedisMessage(final MessageProtos.Envelope message) {
    messagesCache.insert(UUID.fromString(message.getServerGuid()),
        DESTINATION_SERVICE_IDENTIFIER.uuid(),
        destinationDevice.getId(),
        message)
        .join();

    return message;
  }

  private void deleteRedisMessage(final MessageProtos.Envelope message) {
    messagesCache.remove(DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice.getId(), UUID.fromString(message.getServerGuid())).join();
  }

  private MessageProtos.Envelope insertDynamoDbMessage(final MessageProtos.Envelope message) {
    messagesDynamoDb.store(List.of(message), DESTINATION_SERVICE_IDENTIFIER.uuid(), destinationDevice);

    return message;
  }

  private void deleteDynamoDbMessage(final MessageProtos.Envelope message) {
    messagesDynamoDb.deleteMessage(DESTINATION_SERVICE_IDENTIFIER.uuid(),
        destinationDevice,
        UUID.fromString(message.getServerGuid()),
        message.getServerTimestamp())
        .join();
  }

  private static MessageProtos.Envelope generateRandomMessage() {

    final long timestamp = SERIAL_TIMESTAMP.incrementAndGet();

    final MessageProtos.Envelope.Builder envelopeBuilder = MessageProtos.Envelope.newBuilder()
        .setClientTimestamp(timestamp)
        .setServerTimestamp(timestamp)
        .setContent(ByteString.copyFromUtf8(RandomStringUtils.secure().nextAlphanumeric(256)))
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
        .setServerGuid(UUID.randomUUID().toString())
        .setDestinationServiceId(DESTINATION_SERVICE_IDENTIFIER.toServiceIdentifierString());

    return envelopeBuilder.build();
  }
}
