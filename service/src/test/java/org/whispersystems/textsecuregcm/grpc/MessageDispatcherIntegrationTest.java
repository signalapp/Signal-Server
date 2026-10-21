/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.signal.chat.messages.GetMessagesResponse;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.storage.FoundationDbClusterExtension;
import org.whispersystems.textsecuregcm.storage.MessagesDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStore;
import org.whispersystems.textsecuregcm.storage.foundationdb.VersionstampUUIDCipher;
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
  static FoundationDbClusterExtension FOUNDATION_DB_EXTENSION = new FoundationDbClusterExtension(1);

  private static ExecutorService sharedExecutorService;
  private static ScheduledExecutorService sharedScheduledExecutorService;
  private static Scheduler messageDeliveryScheduler;

  private FoundationDbMessageStore foundationDbMessageStore;
  private MessagesDynamoDb messagesDynamoDb;
  private Account account;
  private Device device;
  private MessageDispatcher messageDispatcher;

  private long serialTimestamp = System.currentTimeMillis();

  @BeforeAll
  static void beforeAll() {
    sharedExecutorService = Executors.newSingleThreadExecutor();
    sharedScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");
  }

  @BeforeEach
  void setUp() throws Exception {

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());

    messagesDynamoDb = new MessagesDynamoDb(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(), Tables.MESSAGES.tableName(), Duration.ofDays(7),
        sharedExecutorService);
    account = mock(Account.class);
    device = mock(Device.class);

    final byte[] versionstampCipherKey = new byte[16];
    new SecureRandom().nextBytes(versionstampCipherKey);

    final VersionstampUUIDCipher versionstampUUIDCipher = new VersionstampUUIDCipher(0, versionstampCipherKey);

    foundationDbMessageStore = new FoundationDbMessageStore(
        Map.of(0, Arrays.asList(FOUNDATION_DB_EXTENSION.getDatabases())),
        0,
        versionstampUUIDCipher,
        sharedScheduledExecutorService,
        Clock.systemUTC(),
        Duration.ofSeconds(5),
        10);

    when(account.getNumber()).thenReturn(Optional.of("+18005551234"));
    when(account.getAccountIdentifier()).thenReturn(UUID.randomUUID());
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

    messageDispatcher = new MessageDispatcher(
        mock(ReceiptSender.class),
        new MessagesManager(messagesDynamoDb, foundationDbMessageStore,
            mock(ReportMessageManager.class),
            sharedExecutorService, Clock.systemUTC()),
        new MessageMetrics(),
        mock(PushNotificationManager.class),
        mock(PushNotificationScheduler.class),
        mock(DisconnectionRequestManager.class),
        mock(ClientReleaseManager.class));
  }

  @AfterAll
  static void afterAll() throws InterruptedException {
    sharedExecutorService.shutdown();
    sharedScheduledExecutorService.shutdown();
    final Mono<Void> schedulerShutdownMono = messageDeliveryScheduler.disposeGracefully();

    //noinspection ResultOfMethodCallIgnored
    sharedExecutorService.awaitTermination(2, TimeUnit.SECONDS);

    //noinspection ResultOfMethodCallIgnored
    sharedScheduledExecutorService.awaitTermination(2, TimeUnit.SECONDS);

    schedulerShutdownMono.timeout(Duration.ofSeconds(2))
        .onErrorResume(TimeoutException.class, _ -> Mono.fromRunnable(() -> messageDeliveryScheduler.dispose()))
        .block();
  }


  @ParameterizedTest
  @CsvSource({
      "207, 77",
      "323, 0",
      "0, 87"
  })
  void testProcessStoredMessages(final int dynamoDbMessageCount, final int foundationDbMessageCount) {
    final List<Envelope> expectedMessages = fillRandomMessages(dynamoDbMessageCount, foundationDbMessageCount);
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
    final int dynamoDbMessageCount = 77;

    final List<Envelope> expectedMessages = fillRandomMessages(dynamoDbMessageCount, 0);
    final AtomicInteger remainingMessages = new AtomicInteger(dynamoDbMessageCount);
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
                .submit(() -> expectedMessages.addAll(fillRandomMessages( 0, additionalMessageCount)));
          }
        })
        .take(dynamoDbMessageCount + additionalMessageCount)
        .collectList()
        .block(Duration.ofSeconds(15));
    assertEquals(expectedMessages.size(), receivedMessages.size());
    assertEquals(expectedMessages, receivedMessages);
  }

  @Test
  void testProcessStoredMessagesClientClosed() {
    final int dynamoDbMessageCount = 207;

    final Set<Envelope> receivedMessages = Collections.newSetFromMap(new ConcurrentHashMap<>());
    final List<Envelope> expectedMessages = fillRandomMessages(dynamoDbMessageCount, 0);
    final TestPublisher<UUID> acks = TestPublisher.create();
    final Exception thrown = assertThrows(Exception.class, () -> messageDispatcher
        .getMessages(true, null, account, device, acks.flux())
        .doOnNext(response -> {
          receivedMessages.add(response.getEnvelope());
          acks.error(new IOException("test exception"));
        })
        .collectList()
        .block());

    assertInstanceOf(IOException.class, thrown.getCause());
    assertTrue(expectedMessages.containsAll(receivedMessages));
  }

  private List<Envelope> fillRandomMessages(final int dynamoDbMessageCount, final int foundationDbMessageCount) {

    final List<Envelope> expectedMessages = new ArrayList<>(dynamoDbMessageCount + foundationDbMessageCount);

    for (int i = 0; i < dynamoDbMessageCount; i++) {
      final Envelope envelope = generateRandomMessage(UUID.randomUUID());
      expectedMessages.add(envelope);
    }

    messagesDynamoDb.store(expectedMessages, account.getAccountIdentifier(), device);

    for (int i = 0; i < foundationDbMessageCount; i++) {
      final Envelope envelope = generateRandomMessage(UUID.randomUUID())
          .toBuilder()
          .clearServerGuid()
          .build();

      final UUID messageGuid = foundationDbMessageStore.insert(new AciServiceIdentifier(account.getAccountIdentifier()),
              Map.of(device.getId(), envelope))
          .join()
          .get(device.getId())
          .messageGuid()
          .orElseThrow();

      expectedMessages.add(envelope
          .toBuilder()
          .setServerGuid(UUIDUtil.toByteString(messageGuid))
          .build());
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
