/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
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
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class WebSocketConnectionIntegrationTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.MESSAGES);

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private ExecutorService sharedExecutorService;
  private MessagesDynamoDb messagesDynamoDb;
  private MessagesCache messagesCache;
  private RedisMessageAvailabilityManager redisMessageAvailabilityManager;
  private ReportMessageManager reportMessageManager;
  private Account account;
  private Device device;
  private WebSocketClient webSocketClient;
  private Scheduler messageDeliveryScheduler;
  private ClientReleaseManager clientReleaseManager;

  private long serialTimestamp = System.currentTimeMillis();

  @BeforeEach
  void setUp() throws Exception {
    sharedExecutorService = Executors.newSingleThreadExecutor();
    messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());

    messagesCache = new MessagesCache(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        messageDeliveryScheduler, sharedExecutorService, mock(ScheduledExecutorService.class), Clock.systemUTC(), mock(ExperimentEnrollmentManager.class));
    messagesDynamoDb = new MessagesDynamoDb(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(), Tables.MESSAGES.tableName(), Duration.ofDays(7),
        sharedExecutorService, mock(ExperimentEnrollmentManager.class));
    redisMessageAvailabilityManager = new RedisMessageAvailabilityManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(), sharedExecutorService, sharedExecutorService);
    reportMessageManager = mock(ReportMessageManager.class);
    account = mock(Account.class);
    device = mock(Device.class);
    webSocketClient = mock(WebSocketClient.class);
    clientReleaseManager = mock(ClientReleaseManager.class);

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(UUID.randomUUID());
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

    redisMessageAvailabilityManager.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    redisMessageAvailabilityManager.stop();

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
    final WebSocketConnection webSocketConnection = new WebSocketConnection(
        mock(ReceiptSender.class),
        new MessagesManager(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, reportMessageManager, sharedExecutorService, Clock.systemUTC()),
        new MessageMetrics(),
        mock(PushNotificationManager.class),
        mock(PushNotificationScheduler.class),
        account,
        device,
        webSocketClient,
        messageDeliveryScheduler,
        clientReleaseManager,
        mock(MessageDeliveryLoopMonitor.class),
        mock(ExperimentEnrollmentManager.class)
    );

    final List<MessageProtos.Envelope> expectedMessages = new ArrayList<>(persistedMessageCount + cachedMessageCount);

    assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {

      {
        final List<MessageProtos.Envelope> persistedMessages = new ArrayList<>(persistedMessageCount);

        for (int i = 0; i < persistedMessageCount; i++) {
          final MessageProtos.Envelope envelope = generateRandomMessage(UUID.randomUUID());

          persistedMessages.add(envelope);
          expectedMessages.add(envelope);
        }

        messagesDynamoDb.store(persistedMessages, account.getIdentifier(IdentityType.ACI), device);
      }

      for (int i = 0; i < cachedMessageCount; i++) {
        final UUID messageGuid = UUID.randomUUID();
        final MessageProtos.Envelope envelope = generateRandomMessage(messageGuid);

        messagesCache.insert(messageGuid, account.getIdentifier(IdentityType.ACI), device.getId(), envelope).join();
        expectedMessages.add(envelope);
      }

      final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);

      when(successResponse.getStatus()).thenReturn(200);
      when(webSocketClient.sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), any()))
          .thenReturn(CompletableFuture.completedFuture(successResponse));

      webSocketConnection.start();

      @SuppressWarnings("unchecked") final ArgumentCaptor<Optional<byte[]>> messageBodyCaptor =
          ArgumentCaptor.forClass(Optional.class);

      verify(webSocketClient, timeout(10_000))
          .sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), anyList(), eq(Optional.empty()));

      verify(webSocketClient, times(persistedMessageCount + cachedMessageCount))
          .sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), messageBodyCaptor.capture());

      final List<MessageProtos.Envelope> sentMessages = new ArrayList<>();

      for (final Optional<byte[]> maybeMessageBody : messageBodyCaptor.getAllValues()) {
        maybeMessageBody.ifPresent(messageBytes -> {
          try {
            sentMessages.add(MessageProtos.Envelope.parseFrom(messageBytes));
          } catch (final InvalidProtocolBufferException e) {
            fail("Could not parse sent message");
          }
        });
      }

      assertEquals(expectedMessages, sentMessages);
    });
  }

  @Test
  void testProcessStoredMessagesMultipleSegments() {
    final WebSocketConnection webSocketConnection = new WebSocketConnection(
        mock(ReceiptSender.class),
        new MessagesManager(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, reportMessageManager, sharedExecutorService, Clock.systemUTC()),
        new MessageMetrics(),
        mock(PushNotificationManager.class),
        mock(PushNotificationScheduler.class),
        account,
        device,
        webSocketClient,
        messageDeliveryScheduler,
        clientReleaseManager,
        mock(MessageDeliveryLoopMonitor.class),
        mock(ExperimentEnrollmentManager.class)
    );

    final int persistedMessageCount = 77;
    final int cachedMessageCount = 104;

    final List<MessageProtos.Envelope> expectedMessages = new ArrayList<>(persistedMessageCount + cachedMessageCount);

    assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {

      {
        final List<MessageProtos.Envelope> persistedMessages = new ArrayList<>(persistedMessageCount);

        for (int i = 0; i < persistedMessageCount; i++) {
          final MessageProtos.Envelope envelope = generateRandomMessage(UUID.randomUUID());

          persistedMessages.add(envelope);
          expectedMessages.add(envelope);
        }

        messagesDynamoDb.store(persistedMessages, account.getIdentifier(IdentityType.ACI), device);
      }

      for (int i = 0; i < cachedMessageCount; i++) {
        final UUID messageGuid = UUID.randomUUID();
        final MessageProtos.Envelope envelope = generateRandomMessage(messageGuid);

        messagesCache.insert(messageGuid, account.getIdentifier(IdentityType.ACI), device.getId(), envelope).join();
        expectedMessages.add(envelope);
      }

      final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);

      final AtomicInteger remainingMessages = new AtomicInteger(persistedMessageCount + cachedMessageCount);
      final int additionalMessageCount = 67;

      when(successResponse.getStatus()).thenReturn(200);
      when(webSocketClient.sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), any()))
          .thenAnswer(_ -> {
            if (remainingMessages.addAndGet(-1) == 60) {
              sharedExecutorService.submit(() -> {
                for (int i = 0; i < additionalMessageCount; i++) {
                  final UUID messageGuid = UUID.randomUUID();
                  final MessageProtos.Envelope envelope = generateRandomMessage(messageGuid);

                  messagesCache.insert(messageGuid, account.getIdentifier(IdentityType.ACI), device.getId(), envelope).join();
                  expectedMessages.add(envelope);
                }
              });
            }

            return CompletableFuture.completedFuture(successResponse);
          });

      webSocketConnection.start();

      @SuppressWarnings("unchecked") final ArgumentCaptor<Optional<byte[]>> messageBodyCaptor =
          ArgumentCaptor.forClass(Optional.class);

      verify(webSocketClient, timeout(10_000))
          .sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), anyList(), eq(Optional.empty()));

      verify(webSocketClient, timeout(10_000).times(persistedMessageCount + cachedMessageCount + additionalMessageCount))
          .sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), messageBodyCaptor.capture());

      final List<MessageProtos.Envelope> sentMessages = new ArrayList<>();

      for (final Optional<byte[]> maybeMessageBody : messageBodyCaptor.getAllValues()) {
        maybeMessageBody.ifPresent(messageBytes -> {
          try {
            sentMessages.add(MessageProtos.Envelope.parseFrom(messageBytes));
          } catch (final InvalidProtocolBufferException e) {
            fail("Could not parse sent message");
          }
        });
      }

      assertEquals(expectedMessages, sentMessages);
    });
  }

  @Test
  void testProcessStoredMessagesClientClosed() {
    final WebSocketConnection webSocketConnection = new WebSocketConnection(
        mock(ReceiptSender.class),
        new MessagesManager(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, reportMessageManager, sharedExecutorService, Clock.systemUTC()),
        new MessageMetrics(),
        mock(PushNotificationManager.class),
        mock(PushNotificationScheduler.class),
        account,
        device,
        webSocketClient,
        messageDeliveryScheduler,
        clientReleaseManager,
        mock(MessageDeliveryLoopMonitor.class),
        mock(ExperimentEnrollmentManager.class)
    );

    final int persistedMessageCount = 207;
    final int cachedMessageCount = 173;

    final List<MessageProtos.Envelope> expectedMessages = new ArrayList<>(persistedMessageCount + cachedMessageCount);

    assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {

      {
        final List<MessageProtos.Envelope> persistedMessages = new ArrayList<>(persistedMessageCount);

        for (int i = 0; i < persistedMessageCount; i++) {
          final MessageProtos.Envelope envelope = generateRandomMessage(UUID.randomUUID());
          persistedMessages.add(envelope);
          expectedMessages.add(envelope);
        }

        messagesDynamoDb.store(persistedMessages, account.getIdentifier(IdentityType.ACI), device);
      }

      for (int i = 0; i < cachedMessageCount; i++) {
        final UUID messageGuid = UUID.randomUUID();
        final MessageProtos.Envelope envelope = generateRandomMessage(messageGuid);
        messagesCache.insert(messageGuid, account.getIdentifier(IdentityType.ACI), device.getId(), envelope).join();

        expectedMessages.add(envelope);
      }

      when(webSocketClient.sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), any()))
          .thenReturn(CompletableFuture.failedFuture(new IOException("Connection closed")));

      webSocketConnection.start();

      //noinspection unchecked
      final ArgumentCaptor<Optional<byte[]>> messageBodyCaptor = ArgumentCaptor.forClass(Optional.class);

      verify(webSocketClient, atMost(persistedMessageCount + cachedMessageCount)).sendRequest(eq("PUT"),
          eq("/api/v1/message"), anyList(), messageBodyCaptor.capture());
      verify(webSocketClient, never()).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), anyList(),
          eq(Optional.empty()));

      final List<MessageProtos.Envelope> sentMessages = messageBodyCaptor.getAllValues().stream()
          .map(Optional::orElseThrow)
          .map(messageBytes -> {
            try {
              return Envelope.parseFrom(messageBytes);
            } catch (InvalidProtocolBufferException e) {
              throw new RuntimeException(e);
            }
          }).toList();

      assertTrue(expectedMessages.containsAll(sentMessages));
    });
  }

  private MessageProtos.Envelope generateRandomMessage(final UUID messageGuid) {
    final long timestamp = serialTimestamp++;

    return MessageProtos.Envelope.newBuilder()
        .setClientTimestamp(timestamp)
        .setServerTimestamp(timestamp)
        .setContent(ByteString.copyFromUtf8(RandomStringUtils.secure().nextAlphanumeric(256)))
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
        .setServerGuid(messageGuid.toString())
        .setDestinationServiceId(UUID.randomUUID().toString())
        .build();
  }
}
