/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;

import com.google.protobuf.ByteString;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.ConflictingMessageConsumerException;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessageStream;
import org.whispersystems.textsecuregcm.storage.MessageStreamEntry;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

class WebSocketConnectionTest {

  private Account account;
  private Device device;
  private MessagesManager messagesManager;
  private ReceiptSender receiptSender;
  private Scheduler messageDeliveryScheduler;
  private ClientReleaseManager clientReleaseManager;

  private static final int SOURCE_DEVICE_ID = 1;

  private static final AtomicInteger ON_ERROR_DROPPED_COUNTER = new AtomicInteger();

  @BeforeAll
  static void setUpBeforeAll() {
    Hooks.onErrorDropped(_ -> ON_ERROR_DROPPED_COUNTER.incrementAndGet());
  }

  @BeforeEach
  void setUp() {
    account = mock(Account.class);
    device = mock(Device.class);
    messagesManager = mock(MessagesManager.class);
    receiptSender = mock(ReceiptSender.class);
    messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");
    clientReleaseManager = mock(ClientReleaseManager.class);

    ON_ERROR_DROPPED_COUNTER.set(0);
  }

  @AfterEach
  void tearDown() {
    StepVerifier.resetDefaultTimeout();
    messageDeliveryScheduler.dispose();

    assertEquals(0, ON_ERROR_DROPPED_COUNTER.get(),
        "Errors dropped during test");
  }

  @AfterAll
  static void tearDownAfterAll() {
    Hooks.resetOnErrorDropped();
  }

  private WebSocketConnection buildWebSocketConnection(final WebSocketClient client) {
    return new WebSocketConnection(receiptSender,
        messagesManager,
        new MessageMetrics(),
        mock(PushNotificationManager.class),
        mock(PushNotificationScheduler.class),
        account,
        device,
        client,
        Schedulers.immediate(),
        clientReleaseManager,
        mock(MessageDeliveryLoopMonitor.class),
        mock(ExperimentEnrollmentManager.class));
  }

  @Test
  void testSendMessages() {

    final UUID destinationAccountIdentifier = UUID.randomUUID();
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(destinationAccountIdentifier);

    final byte deviceId = 2;
    when(device.getId()).thenReturn(deviceId);

    final Envelope successfulMessage = createMessage(UUID.randomUUID(), destinationAccountIdentifier, 1, "Success");
    final Envelope secondSuccessfulMessage = createMessage(UUID.randomUUID(), destinationAccountIdentifier, 2, "Second success");

    final MessageStream messageStream = mock(MessageStream.class);

    when(messageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.just(
            new MessageStreamEntry.Envelope(successfulMessage),
            new MessageStreamEntry.QueueEmpty(),
            new MessageStreamEntry.Envelope(secondSuccessfulMessage))));

    when(messageStream.acknowledgeMessage(any())).thenReturn(CompletableFuture.completedFuture(null));

    when(messagesManager.getMessages(account.getIdentifier(IdentityType.ACI), device))
        .thenReturn(messageStream);

    when(messagesManager.mayHaveMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(false));

    final WebSocketClient client = mock(WebSocketClient.class);

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    when(client.isOpen()).thenReturn(true);

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(successResponse));

    final WebSocketConnection webSocketConnection = buildWebSocketConnection(client);
    webSocketConnection.start();

    verify(client).sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), argThat(body ->
        body.isPresent() && Arrays.equals(body.get(), WebSocketConnection.serializeMessage(successfulMessage))));

    verify(client).sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), argThat(body ->
        body.isPresent() && Arrays.equals(body.get(), WebSocketConnection.serializeMessage(secondSuccessfulMessage))));

    verify(messageStream).acknowledgeMessage(successfulMessage);
    verify(messageStream).acknowledgeMessage(secondSuccessfulMessage);

    verify(receiptSender)
        .sendReceipt(new AciServiceIdentifier(destinationAccountIdentifier),
            deviceId,
            AciServiceIdentifier.valueOf(successfulMessage.getSourceServiceId()),
            successfulMessage.getClientTimestamp());

    verify(receiptSender)
        .sendReceipt(new AciServiceIdentifier(destinationAccountIdentifier),
            deviceId,
            AciServiceIdentifier.valueOf(secondSuccessfulMessage.getSourceServiceId()),
            secondSuccessfulMessage.getClientTimestamp());

    webSocketConnection.stop();

    verify(client).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), anyList(), eq(Optional.empty()));
    verify(client).close(eq(1000), anyString());
  }

  @Test
  void testSendMessagesWithError() {

    final UUID destinationAccountIdentifier = UUID.randomUUID();
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(destinationAccountIdentifier);

    final byte deviceId = 2;
    when(device.getId()).thenReturn(deviceId);

    final Envelope successfulMessage = createMessage(UUID.randomUUID(), destinationAccountIdentifier, 1, "Success");
    final Envelope failedMessage = createMessage(UUID.randomUUID(), destinationAccountIdentifier, 2, "Failed");
    final Envelope secondSuccessfulMessage = createMessage(UUID.randomUUID(), destinationAccountIdentifier, 3, "Second success");

    final MessageStream messageStream = mock(MessageStream.class);

    when(messageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.just(
            new MessageStreamEntry.Envelope(successfulMessage),
            new MessageStreamEntry.Envelope(failedMessage),
            new MessageStreamEntry.QueueEmpty(),
            new MessageStreamEntry.Envelope(secondSuccessfulMessage))));

    when(messageStream.acknowledgeMessage(any())).thenReturn(CompletableFuture.completedFuture(null));

    when(messagesManager.getMessages(account.getIdentifier(IdentityType.ACI), device))
        .thenReturn(messageStream);

    when(messagesManager.mayHaveMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(false));

    final WebSocketClient client = mock(WebSocketClient.class);

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    when(client.isOpen()).thenReturn(true);

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(successResponse));

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(), argThat(body ->
        body.isPresent() && Arrays.equals(body.get(), WebSocketConnection.serializeMessage(failedMessage)))))
        .thenReturn(CompletableFuture.failedFuture(new RedisCommandTimeoutException()));

    final WebSocketConnection webSocketConnection = buildWebSocketConnection(client);
    webSocketConnection.start();

    verify(client).sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), argThat(body ->
        body.isPresent() && Arrays.equals(body.get(), WebSocketConnection.serializeMessage(successfulMessage))));

    verify(client).sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), argThat(body ->
        body.isPresent() && Arrays.equals(body.get(), WebSocketConnection.serializeMessage(failedMessage))));

    verify(client, never()).sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), argThat(body ->
        body.isPresent() && Arrays.equals(body.get(), WebSocketConnection.serializeMessage(secondSuccessfulMessage))));

    verify(messageStream).acknowledgeMessage(successfulMessage);
    verify(messageStream, never()).acknowledgeMessage(secondSuccessfulMessage);

    verify(receiptSender)
        .sendReceipt(new AciServiceIdentifier(destinationAccountIdentifier),
            deviceId,
            AciServiceIdentifier.valueOf(successfulMessage.getSourceServiceId()),
            successfulMessage.getClientTimestamp());

    verify(receiptSender, never())
        .sendReceipt(new AciServiceIdentifier(destinationAccountIdentifier),
            deviceId,
            AciServiceIdentifier.valueOf(failedMessage.getSourceServiceId()),
            failedMessage.getClientTimestamp());

    verify(receiptSender, never())
        .sendReceipt(new AciServiceIdentifier(destinationAccountIdentifier),
            deviceId,
            AciServiceIdentifier.valueOf(secondSuccessfulMessage.getSourceServiceId()),
            secondSuccessfulMessage.getClientTimestamp());

    verify(client, timeout(500)).close(eq(1011), anyString());
    verify(client, never()).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), anyList(), eq(Optional.empty()));
  }

  @Test
  void testQueueEmptySignalOrder() {

    final UUID destinationAccountIdentifier = UUID.randomUUID();
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(destinationAccountIdentifier);

    final byte deviceId = 2;
    when(device.getId()).thenReturn(deviceId);

    final Envelope initialMessage = createMessage(UUID.randomUUID(), destinationAccountIdentifier, 1, "Initial message");
    final Envelope afterQueueDrainMessage = createMessage(UUID.randomUUID(), destinationAccountIdentifier, 2, "After queue drained");

    final MessageStream messageStream = mock(MessageStream.class);

    when(messageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.just(
            new MessageStreamEntry.Envelope(initialMessage),
            new MessageStreamEntry.QueueEmpty(),
            new MessageStreamEntry.Envelope(afterQueueDrainMessage))));

    when(messageStream.acknowledgeMessage(any())).thenReturn(CompletableFuture.completedFuture(null));

    when(messagesManager.getMessages(account.getIdentifier(IdentityType.ACI), device))
        .thenReturn(messageStream);

    when(messagesManager.mayHaveMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(false));

    final WebSocketClient client = mock(WebSocketClient.class);

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(), any()))
        .thenAnswer(_ -> CompletableFuture.supplyAsync(() -> successResponse,
            CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)));

    final WebSocketConnection webSocketConnection = buildWebSocketConnection(client);
    webSocketConnection.start();

    final InOrder inOrder = inOrder(client, messageStream);

    // Sending the initial message will succeed after a delay, at which point we'll acknowledge the message. Make sure
    // we wait for that process to complete before sending the "queue empty" signal
    inOrder.verify(messageStream, timeout(1_000)).acknowledgeMessage(initialMessage);
    inOrder.verify(client, timeout(1_000)).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), anyList(), eq(Optional.empty()));

    webSocketConnection.stop();
    verify(client).close(eq(1000), anyString());
  }

  @Test
  void testConflictingConsumerSignalOrder() {

    final UUID destinationAccountIdentifier = UUID.randomUUID();
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(destinationAccountIdentifier);

    final byte deviceId = 2;
    when(device.getId()).thenReturn(deviceId);

    final Envelope message = createMessage(UUID.randomUUID(), destinationAccountIdentifier, 1, "Initial message");

    final MessageStream messageStream = mock(MessageStream.class);
    final TestPublisher<MessageStreamEntry> testPublisher = TestPublisher.createCold();

    when(messageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(testPublisher));

    when(messageStream.acknowledgeMessage(any())).thenReturn(CompletableFuture.completedFuture(null));

    when(messagesManager.getMessages(account.getIdentifier(IdentityType.ACI), device))
        .thenReturn(messageStream);

    when(messagesManager.mayHaveMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(false));

    final WebSocketClient client = mock(WebSocketClient.class);

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(), any()))
        .thenReturn(new CompletableFuture<>());

    final WebSocketConnection webSocketConnection = buildWebSocketConnection(client);
    webSocketConnection.start();

    testPublisher.next(new MessageStreamEntry.Envelope(message));
    testPublisher.error(new ConflictingMessageConsumerException());

    final InOrder inOrder = inOrder(client, messageStream);

    // A "conflicting consumer" should close the socket as soon as possible (i.e. even if messages are still getting
    // processed)
    inOrder.verify(client).sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), argThat(body ->
        body.isPresent() && Arrays.equals(body.get(), WebSocketConnection.serializeMessage(message))));

    verify(client).close(eq(4409), anyString());
  }

  @Test
  void testSendMessagesEmptyQueue() {
    final UUID accountUuid = UUID.randomUUID();

    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

    final MessageStream messageStream = mock(MessageStream.class);

    when(messageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.just(new MessageStreamEntry.QueueEmpty())));

    when(messageStream.acknowledgeMessage(any())).thenReturn(CompletableFuture.completedFuture(null));

    when(messagesManager.getMessages(accountUuid, device)).thenReturn(messageStream);

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(true);

    final WebSocketConnection webSocketConnection = buildWebSocketConnection(client);

    webSocketConnection.start();

    verify(client, timeout(1_000)).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), anyList(), eq(Optional.empty()));
  }

  @Test
  void testSendMessagesConflictingConsumer() {
    final UUID accountUuid = UUID.randomUUID();

    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

    final MessageStream messageStream = mock(MessageStream.class);

    when(messageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.error(new ConflictingMessageConsumerException())));

    when(messageStream.acknowledgeMessage(any())).thenReturn(CompletableFuture.completedFuture(null));

    when(messagesManager.getMessages(accountUuid, device)).thenReturn(messageStream);

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(true);

    final WebSocketConnection webSocketConnection = buildWebSocketConnection(client);
    webSocketConnection.start();

    verify(client, timeout(1_000)).close(eq(4409), anyString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSendMessagesRetrievalException(final boolean clientOpen) {
    final UUID accountUuid = UUID.randomUUID();

    when(device.getId()).thenReturn((byte) 2);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);

    final MessageStream messageStream = mock(MessageStream.class);

    when(messageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.error(new RedisException("OH NO"))));

    when(messageStream.acknowledgeMessage(any())).thenReturn(CompletableFuture.completedFuture(null));

    when(messagesManager.getMessages(accountUuid, device)).thenReturn(messageStream);

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(clientOpen);

    final WebSocketConnection webSocketConnection = buildWebSocketConnection(client);
    webSocketConnection.start();

    if (clientOpen) {
      verify(client).close(eq(1011), anyString());
    } else {
      verify(client, never()).close(anyInt(), any());
    }
  }

  @Test
  void testReactivePublisherLimitRate() {
    final UUID accountUuid = UUID.randomUUID();

    final byte deviceId = 2;
    when(device.getId()).thenReturn(deviceId);

    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);

    final int totalMessages = 1000;

    final TestPublisher<MessageStreamEntry> testPublisher = TestPublisher.createCold();
    final Flux<MessageStreamEntry> flux = Flux.from(testPublisher);

    final MessageStream messageStream = mock(MessageStream.class);

    when(messageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(flux));

    when(messageStream.acknowledgeMessage(any())).thenReturn(CompletableFuture.completedFuture(null));

    when(messagesManager.getMessages(accountUuid, device)).thenReturn(messageStream);

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(true);
    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);
    when(client.sendRequest(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

    final WebSocketConnection webSocketConnection = buildWebSocketConnection(client);

    webSocketConnection.start();

    StepVerifier.setDefaultTimeout(Duration.ofSeconds(5));

    StepVerifier.create(flux, 0)
        .expectSubscription()
        .thenRequest(totalMessages * 2)
        .then(() -> {
          for (long i = 0; i < totalMessages; i++) {
            testPublisher.next(new MessageStreamEntry.Envelope(createMessage(UUID.randomUUID(), accountUuid, 1111 * i + 1, "message " + i)));
          }
          testPublisher.complete();
        })
        .expectNextCount(totalMessages)
        .expectComplete()
        .log()
        .verify();

    testPublisher.assertMaxRequested(WebSocketConnection.MESSAGE_PUBLISHER_LIMIT_RATE);
  }

  @Test
  void testReactivePublisherDisposedWhenConnectionStopped() {
    final UUID accountUuid = UUID.randomUUID();

    final byte deviceId = 2;
    when(device.getId()).thenReturn(deviceId);

    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);

    final AtomicBoolean canceled = new AtomicBoolean();

    final Flux<MessageStreamEntry> flux = Flux.create(s -> {
      s.onRequest(n -> {
        // the subscriber should request more than 1 message, but we will only send one, so that
        // we are sure the subscriber is waiting for more when we stop the connection
        assert n > 1;
        s.next(new MessageStreamEntry.Envelope(createMessage(UUID.randomUUID(), UUID.randomUUID(), 1111, "first")));
      });

      s.onCancel(() -> canceled.set(true));
    });

    final MessageStream messageStream = mock(MessageStream.class);

    when(messageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(flux));

    when(messageStream.acknowledgeMessage(any())).thenReturn(CompletableFuture.completedFuture(null));

    when(messagesManager.getMessages(accountUuid, device)).thenReturn(messageStream);
    when(messagesManager.mayHaveMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(false));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(true);
    when(client.sendRequest(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

    final WebSocketConnection webSocketConnection = buildWebSocketConnection(client);

    webSocketConnection.start();

    verify(client).sendRequest(any(), any(), any(), any());

    // close the connection before the publisher completes
    webSocketConnection.stop();

    StepVerifier.setDefaultTimeout(Duration.ofSeconds(2));

    StepVerifier.create(flux)
        .expectSubscription()
        .expectNextCount(1)
        .then(() -> assertTrue(canceled.get()))
        // this is not entirely intuitive, but expecting a timeout is the recommendation for verifying cancellation
        .expectTimeout(Duration.ofMillis(100))
        .log()
        .verify();
  }

  private static Envelope createMessage(final UUID senderUuid,
      final UUID destinationUuid,
      final long timestamp,
      final String content) {

    return Envelope.newBuilder()
        .setServerGuid(UUID.randomUUID().toString())
        .setType(Envelope.Type.CIPHERTEXT)
        .setClientTimestamp(timestamp)
        .setServerTimestamp(0)
        .setSourceServiceId(senderUuid.toString())
        .setSourceDevice(SOURCE_DEVICE_ID)
        .setDestinationServiceId(destinationUuid.toString())
        .setContent(ByteString.copyFrom(content.getBytes(StandardCharsets.UTF_8)))
        .build();
  }

}
