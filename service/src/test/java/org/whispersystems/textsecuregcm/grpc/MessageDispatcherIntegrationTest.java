/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.signal.chat.messages.GetMessagesRequest;
import org.signal.chat.messages.GetMessagesResponse;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.push.RedisMessageAvailabilityManager;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.storage.MessagesCache;
import org.whispersystems.textsecuregcm.storage.MessagesDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.publisher.TestPublisher;

@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class MessageDispatcherIntegrationTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.MESSAGES);

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private static ExecutorService sharedExecutorService;
  private static Scheduler messageDeliveryScheduler;

  private MessagesDynamoDb messagesDynamoDb;
  private MessagesCache messagesCache;
  private RedisMessageAvailabilityManager redisMessageAvailabilityManager;
  private ReportMessageManager reportMessageManager;
  private Account account;
  private Device device;
  private ClientReleaseManager clientReleaseManager;
  private MessageDispatcher messageDispatcher;

  private long serialTimestamp = System.currentTimeMillis();

  @BeforeAll
  static void beforeAll() {
    sharedExecutorService = Executors.newSingleThreadExecutor();
    messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");
  }

  @BeforeEach
  void setUp() throws Exception {

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());

    messagesCache = new MessagesCache(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        messageDeliveryScheduler, sharedExecutorService, mock(ScheduledExecutorService.class), Clock.systemUTC());
    messagesDynamoDb = new MessagesDynamoDb(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(), Tables.MESSAGES.tableName(), Duration.ofDays(7),
        sharedExecutorService);
    redisMessageAvailabilityManager = new RedisMessageAvailabilityManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        sharedExecutorService, sharedExecutorService);
    reportMessageManager = mock(ReportMessageManager.class);
    account = mock(Account.class);
    device = mock(Device.class);
    clientReleaseManager = mock(ClientReleaseManager.class);

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(UUID.randomUUID());
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

    redisMessageAvailabilityManager.start();

    messageDispatcher = new MessageDispatcher(
        mock(ReceiptSender.class),
        new MessagesManager(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, reportMessageManager,
            sharedExecutorService, Clock.systemUTC()),
        new MessageMetrics(),
        mock(PushNotificationManager.class),
        mock(PushNotificationScheduler.class),
        mock(MessageDeliveryLoopMonitor.class),
        mock(DisconnectionRequestManager.class),
        clientReleaseManager);
  }

  @AfterEach
  void tearDown() throws Exception {
    redisMessageAvailabilityManager.stop();

  }

  @AfterAll
  static void afterAll() throws InterruptedException {
    sharedExecutorService.shutdown();
    final Mono<Void> schedulerShutdownMono = messageDeliveryScheduler.disposeGracefully();

    //noinspection ResultOfMethodCallIgnored
    sharedExecutorService.awaitTermination(2, TimeUnit.SECONDS);
    schedulerShutdownMono.timeout(Duration.ofSeconds(2))
        .onErrorResume(TimeoutException.class, _ -> Mono.fromRunnable(() -> messageDeliveryScheduler.dispose()))
        .block();
  }


  @ParameterizedTest
  @CsvSource({
      "207, 173",
      "323, 0",
      "0, 221",
  })
  void testProcessStoredMessages(final int persistedMessageCount, final int cachedMessageCount) {
    final List<Envelope> expectedMessages = fillRandomMessages(persistedMessageCount, cachedMessageCount);
    final Sinks.Many<UUID> acks = Sinks.many().unicast().onBackpressureBuffer();
    final List<Envelope> receivedMessages = messageDispatcher
        .getMessages(true, null, account, device, acks.asFlux())
        .takeWhile(response -> !response.hasQueueEmpty())
        .doOnNext(response -> acks.tryEmitNext(UUIDUtil.fromByteString(response.getEnvelope().getServerGuid())))
        .map(GetMessagesResponse::getEnvelope)
        .collectList()
        .block(Duration.ofSeconds(15));
    assertEquals(expectedMessages, receivedMessages);
  }

  @Test
  void testProcessStoredMessagesMultipleSegments() {
    final int persistedMessageCount = 77;
    final int cachedMessageCount = 104;

    final List<Envelope> expectedMessages = fillRandomMessages(persistedMessageCount, cachedMessageCount);
    final AtomicInteger remainingMessages = new AtomicInteger(persistedMessageCount + cachedMessageCount);
    final int additionalMessageCount = 67;

    final Sinks.Many<UUID> acks = Sinks.many().unicast().onBackpressureBuffer();
    final List<Envelope> receivedMessages = messageDispatcher
        .getMessages(true, null, account, device, acks.asFlux())
        .filter(response -> !response.hasQueueEmpty())
        .map(GetMessagesResponse::getEnvelope)
        .doOnNext(envelope -> {
          acks.tryEmitNext(UUIDUtil.fromByteString(envelope.getServerGuid()));
          if (remainingMessages.addAndGet(-1) == 60) {
            sharedExecutorService
                .submit(() -> expectedMessages.addAll(fillRandomMessages(0, additionalMessageCount)));
          }
        })
        .take(persistedMessageCount +  cachedMessageCount + additionalMessageCount)
        .collectList()
        .block(Duration.ofSeconds(15));
    assertEquals(expectedMessages.size(), receivedMessages.size());
    assertEquals(expectedMessages, receivedMessages);
  }

  @Test
  void testProcessStoredMessagesClientClosed() {
    final int persistedMessageCount = 207;
    final int cachedMessageCount = 173;

    final Set<Envelope> receivedMessages = Collections.newSetFromMap(new ConcurrentHashMap<Envelope, Boolean>());
    final List<Envelope> expectedMessages = fillRandomMessages(persistedMessageCount, cachedMessageCount);
    final TestPublisher<UUID> acks = TestPublisher.create();
    final Exception thrown = assertThrows(Exception.class, () -> messageDispatcher
        .getMessages(true, null, account, device, acks.flux())
        .doOnNext(response -> {
          assertTrue(redisMessageAvailabilityManager.isLocallyPresent(account.getIdentifier(IdentityType.ACI),
              device.getId()));
          receivedMessages.add(response.getEnvelope());
          acks.error(new IOException("test exception"));
        })
        .collectList()
        .block());
    assertInstanceOf(IOException.class, thrown.getCause());
    assertTrue(expectedMessages.containsAll(receivedMessages));
    assertFalse(redisMessageAvailabilityManager.isLocallyPresent(account.getIdentifier(IdentityType.ACI), device.getId()));
  }

  private List<Envelope> fillRandomMessages(final int persistedMessageCount, final int cachedMessageCount) {

    final List<Envelope> expectedMessages = new ArrayList<>(persistedMessageCount + cachedMessageCount);
    for (int i = 0; i < persistedMessageCount; i++) {
      final Envelope envelope = generateRandomMessage(UUID.randomUUID());
      expectedMessages.add(envelope);
    }
    messagesDynamoDb.store(expectedMessages, account.getIdentifier(IdentityType.ACI), device);

    for (int i = 0; i < cachedMessageCount; i++) {
      final UUID messageGuid = UUID.randomUUID();
      final Envelope envelope = generateRandomMessage(messageGuid);

      messagesCache.insert(messageGuid, account.getIdentifier(IdentityType.ACI), device.getId(), envelope).join();
      expectedMessages.add(envelope);
    }
    return expectedMessages;
  }

  private Envelope generateRandomMessage(final UUID messageGuid) {
    final long timestamp = serialTimestamp++;

    return Envelope.newBuilder()
        .setClientTimestamp(timestamp)
        .setServerTimestamp(timestamp)
        .setContent(ByteString.copyFromUtf8(RandomStringUtils.secure().nextAlphanumeric(256)))
        .setType(Envelope.Type.CIPHERTEXT)
        .setServerGuid(new AciServiceIdentifier(messageGuid).toCompactByteString())
        .setDestinationServiceId(new AciServiceIdentifier(UUID.randomUUID()).toCompactByteString())
        .build();
  }
}
