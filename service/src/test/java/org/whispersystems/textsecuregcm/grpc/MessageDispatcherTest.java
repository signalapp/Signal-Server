/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.signal.chat.messages.GetMessagesResponse;
import org.signal.chat.messages.GetMessagesStreamClosed;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestListener;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
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
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

class MessageDispatcherTest {

  private static final GetMessagesResponse QUEUE_EMPTY =
      GetMessagesResponse.newBuilder().setQueueEmpty(Empty.getDefaultInstance()).build();

  private static final UUID ACI = UUID.randomUUID();
  private static final byte DEVICE_ID = 2;
  private static final int SOURCE_DEVICE_ID = 1;
  private static final String TEST_UA = "test-user-agent";

  private Account account;
  private Device device;
  private MessagesManager messagesManager;
  private ReceiptSender receiptSender;
  private PushNotificationManager pushNotificationManager;
  private MessageDispatcher dispatcher;
  private DisconnectionRequestManager disconnectionRequestManager;
  private PushNotificationScheduler pushNotificationScheduler;
  private MessageDeliveryLoopMonitor messageDeliveryLoopMonitor;


  @BeforeEach
  void setUp() {
    account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(ACI);
    when(account.getUuid()).thenReturn(ACI);

    device = mock(Device.class);
    messagesManager = mock(MessagesManager.class);
    receiptSender = mock(ReceiptSender.class);
    pushNotificationManager = mock(PushNotificationManager.class);
    disconnectionRequestManager = mock(DisconnectionRequestManager.class);
    pushNotificationScheduler = mock(PushNotificationScheduler.class);
    messageDeliveryLoopMonitor = mock(MessageDeliveryLoopMonitor.class);

    when(device.getId()).thenReturn(DEVICE_ID);
    when(device.isPrimary()).thenReturn(true);

    dispatcher = new MessageDispatcher(
        receiptSender,
        messagesManager,
        new MessageMetrics(),
        pushNotificationManager,
        pushNotificationScheduler,
        messageDeliveryLoopMonitor,
        disconnectionRequestManager,
        mock(ClientReleaseManager.class));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void drainSingleMessage(final boolean mayHaveMoreMessages) {
    final ServiceIdentifier sourceServiceId = new AciServiceIdentifier(UUID.randomUUID());
    final long timestamp = 17;

    final Envelope message = createMessage(sourceServiceId, ACI, timestamp, "hello");
    mockMessages(message);

    final Flux<GetMessagesResponse> responses =
        dispatcher.getMessages(true, TEST_UA, account, device, Flux.never());
    final Envelope expectedEnvelope = message.toBuilder().clearEphemeral().build();

    when(messagesManager.mayHaveMessages(any(), any())).thenReturn(
        CompletableFuture.completedFuture(mayHaveMoreMessages));

    StepVerifier.create(responses)
        .expectNext(GetMessagesResponse.newBuilder().setEnvelope(expectedEnvelope).build())
        .then(() -> verify(disconnectionRequestManager).addListener(eq(ACI), eq(DEVICE_ID), any()))
        .thenCancel()
        .verify(Duration.ofSeconds(5));

    verify(messageDeliveryLoopMonitor, times(1))
        .recordDeliveryAttempt(ACI, DEVICE_ID, UUIDUtil.fromByteString(message.getServerGuid()), TEST_UA, "grpc");
    verify(disconnectionRequestManager).removeListener(eq(ACI), eq(DEVICE_ID), any());
    verify(pushNotificationManager).handleMessagesRetrieved(account, device, TEST_UA);
    verify(messagesManager).mayHaveMessages(ACI, device);
    verify(pushNotificationScheduler, times(mayHaveMoreMessages ? 1 : 0))
        .scheduleDelayedNotification(account, device, MessageDispatcher.CLOSE_WITH_PENDING_MESSAGES_NOTIFICATION_DELAY);
    verifyNoInteractions(receiptSender);
  }

  @Test
  void ackMessages() {
    final AciServiceIdentifier sourceServiceId = new AciServiceIdentifier(UUID.randomUUID());

    final List<Envelope> messages = IntStream
        .range(0, MessageDispatcher.MAX_UNACKED_MESSAGES + 2)
        .mapToObj(i -> createMessage(sourceServiceId, ACI, i, "hello"))
        .toList();
    final List<UUID> guids = messages.stream().map(Envelope::getServerGuid).map(UUIDUtil::fromByteString).toList();
    final List<Long> timestamps = messages.stream().map(Envelope::getServerTimestamp).toList();
    final List<GetMessagesResponse> expectedMessages = messages.stream()
        .map(e -> e.toBuilder().clearEphemeral().build())
        .map(e -> GetMessagesResponse.newBuilder().setEnvelope(e).build())
        .toList();

    final MessageStream messageStream = mockMessages(messages);

    final TestPublisher<UUID> acks = TestPublisher.create();

    final Flux<GetMessagesResponse> responses =
        dispatcher.getMessages(true, TEST_UA, account, device, acks.flux());

    StepVerifier.create(responses)
        .expectNext(expectedMessages.subList(0, MessageDispatcher.MAX_UNACKED_MESSAGES).toArray(GetMessagesResponse[]::new))
        .expectNoEvent(Duration.ofMillis(1))

        // Sending an ack should allow another message to come back
        .then(() -> acks.next(guids.get(0)))
        .expectNext(expectedMessages.get(MessageDispatcher.MAX_UNACKED_MESSAGES))
        .then(() -> verify(messageStream, times(1)).acknowledgeMessage(guids.get(0), timestamps.get(0)))
        .expectNoEvent(Duration.ofMillis(1))

        // Send another one
        .then(() -> acks.next(guids.get(1)))
        .expectNext(expectedMessages.get(MessageDispatcher.MAX_UNACKED_MESSAGES + 1))
        .then(() -> verify(messageStream, times(1)).acknowledgeMessage(guids.get(1), timestamps.get(1)))
        .expectNoEvent(Duration.ofMillis(1))

        .thenCancel()
        .verify(Duration.ofSeconds(5));

    verify(receiptSender, times(1))
        .sendReceipt(new AciServiceIdentifier(ACI), DEVICE_ID, sourceServiceId, 0);
    verify(receiptSender, times(1))
        .sendReceipt(new AciServiceIdentifier(ACI), DEVICE_ID, sourceServiceId, 1);
    verifyNoMoreInteractions(receiptSender);
  }

  @Test
  void maxOutstandingAllowsGaps() {
    final AciServiceIdentifier sourceServiceId = new AciServiceIdentifier(UUID.randomUUID());

    final int totalMessages = MessageDispatcher.MAX_UNACKED_MESSAGES  * 2;

    final List<Envelope> messages = IntStream
        .range(0, totalMessages)
        .mapToObj(i -> createMessage(sourceServiceId, ACI, i, "hello"))
        .toList();
    final List<UUID> ackRequests = messages.stream()
        .map(Envelope::getServerGuid).map(UUIDUtil::fromByteString)
        .toList();
    final List<GetMessagesResponse> expectedMessages = messages.stream()
        .map(e -> e.toBuilder().clearEphemeral().build())
        .map(e -> GetMessagesResponse.newBuilder().setEnvelope(e).build())
        .toList();

    mockMessages(messages);
    final TestPublisher<UUID> acks = TestPublisher.create();

    final Flux<GetMessagesResponse> responses =
        dispatcher.getMessages(true, TEST_UA, account, device, acks.flux());

    StepVerifier.create(responses)
        .expectNext(expectedMessages.subList(0, MessageDispatcher.MAX_UNACKED_MESSAGES).toArray(GetMessagesResponse[]::new))
        .expectNoEvent(Duration.ofMillis(1))
        .then(() -> {
          for (int i = 0; i < MessageDispatcher.MAX_UNACKED_MESSAGES; i++) {
            if (i != 2) {
              acks.next(ackRequests.get(i));
            }
          }
        })
        .expectNext(expectedMessages.subList(MessageDispatcher.MAX_UNACKED_MESSAGES, totalMessages - 1).toArray(GetMessagesResponse[]::new))
        .expectNoEvent(Duration.ofMillis(1))
        .then(() -> acks.next(ackRequests.get(2)))
        .expectNext(expectedMessages.get(totalMessages - 1))
        .expectNoEvent(Duration.ofMillis(1))
        .then(() -> {
          for (int i = MessageDispatcher.MAX_UNACKED_MESSAGES; i < totalMessages; i++) {
            acks.next(ackRequests.get(i));
          }
        })
        .expectNext(QUEUE_EMPTY)
        .thenCancel()
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void duplicateAckIgnored() {
    final AciServiceIdentifier sourceServiceId = new AciServiceIdentifier(UUID.randomUUID());
    final List<Envelope> messages = IntStream
        .range(0, MessageDispatcher.MAX_UNACKED_MESSAGES + 2)
        .mapToObj(i -> createMessage(sourceServiceId, ACI, i, "hello"))
        .toList();

    final List<UUID> ackRequests = messages.stream()
        .map(Envelope::getServerGuid).map(UUIDUtil::fromByteString)
        .toList();
    final TestPublisher<UUID> acks = TestPublisher.create();

    mockMessages(messages);

    StepVerifier
        .create(dispatcher.getMessages(true, TEST_UA, account, device, acks.flux()))
        .expectNextCount(MessageDispatcher.MAX_UNACKED_MESSAGES)
        .expectNoEvent(Duration.ofMillis(1))

        // Ack a message, we should then get a new message
        .then(() -> acks.next(ackRequests.get(0)))
        .expectNextCount(1)

        // Ack the same message again, there should be no additional message (or error)
        .then(() -> acks.next(ackRequests.get(0)))
        .expectNoEvent(Duration.ofMillis(1))

        // Ack a different message, we should get a message back
        .then(() -> acks.next(ackRequests.get(1)))
        .expectNextCount(1)
        .thenCancel()
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void disconnectionRequest() {
    final ServiceIdentifier sourceServiceId = new AciServiceIdentifier(UUID.randomUUID());
    final long timestamp = 17;

    final Envelope message = createMessage(sourceServiceId, ACI, timestamp, "hello");
    final Envelope expectedEnvelope = message.toBuilder().clearEphemeral().build();
    mockMessages(message);

    final Flux<GetMessagesResponse> responses =
        dispatcher.getMessages(true, TEST_UA, account, device, Flux.never());
    ArgumentCaptor<DisconnectionRequestListener> listener = ArgumentCaptor.forClass(DisconnectionRequestListener.class);
    StepVerifier.create(responses)
        .expectNext(GetMessagesResponse.newBuilder().setEnvelope(expectedEnvelope).build())
        .then(() -> {
          verify(disconnectionRequestManager).addListener(eq(ACI), eq(DEVICE_ID), listener.capture());
          listener.getValue().handleDisconnectionRequest();
        })
        .expectErrorSatisfies(MessageDispatcherTest::assertReauthError)
        .verify();
  }

  private static void assertReauthError(final Throwable e) {
    GrpcTestUtils.assertStatusException(Status.UNAUTHENTICATED, "INVALID_CREDENTIALS", e);
  }

  private static void assertConflictError(final Throwable e) {
    final GetMessagesStreamClosed reason = GrpcTestUtils.assertStreamClosed(GetMessagesStreamClosed.class, e);
    assertEquals(GetMessagesStreamClosed.ReasonCase.CONFLICTING_STREAM, reason.getReasonCase());
  }

  @Test
  void conflictingConsumer() {
    mockMessages(Flux.error(new ConflictingMessageConsumerException()));
    final Flux<GetMessagesResponse> responses =
        dispatcher.getMessages(true, TEST_UA, account, device, Flux.never());
    StepVerifier.create(responses)
        .expectErrorSatisfies(MessageDispatcherTest::assertConflictError)
        .verify();
  }

  @Test
  void conflictingConsumerWhileWaitingForAcks() {
    final AciServiceIdentifier sourceServiceId = new AciServiceIdentifier(UUID.randomUUID());
    final List<MessageStreamEntry.Envelope> messages = IntStream
        .range(0, MessageDispatcher.MAX_UNACKED_MESSAGES + 2)
        .mapToObj(i -> createMessage(sourceServiceId, ACI, i, "hello"))
        .map(MessageStreamEntry.Envelope::new)
        .toList();

    final TestPublisher<MessageStreamEntry> source = TestPublisher.create();
    mockMessages(source.flux());

    StepVerifier
        .create(dispatcher.getMessages(true, TEST_UA, account, device, Flux.never()))
        .then(() -> messages.forEach(source::next))
        .expectNextCount(MessageDispatcher.MAX_UNACKED_MESSAGES)
        .expectNoEvent(Duration.ofMillis(1))
        // The error should make it to us immediately even though there are max-unacked messages outstanding
        .then(() -> source.error(new ConflictingMessageConsumerException()))
        .expectErrorSatisfies(MessageDispatcherTest::assertConflictError)
        .verify();
  }

  /// Set envelopes to return when a message stream is created. A [MessageStreamEntry.QueueEmpty] will be emitted after
  /// the provided messages
  private MessageStream mockMessages(final List<MessageProtos.Envelope> messages) {
    return mockMessages(messages.toArray(MessageProtos.Envelope[]::new));
  }

  /// Set envelopes to return when a message stream is created. A [MessageStreamEntry.QueueEmpty] will be emitted after
  /// the provided messages
  private MessageStream mockMessages(final MessageProtos.Envelope... messages) {
    return mockMessages(Flux.concat(
        Flux.fromStream(Arrays.stream(messages).map(MessageStreamEntry.Envelope::new)),
        Flux.just(new MessageStreamEntry.QueueEmpty())));
  }

  private MessageStream mockMessages(final Flux<MessageStreamEntry> messages) {
    final MessageStream mockMessageStream = mock(MessageStream.class);
    when(mockMessageStream.getMessages()).thenReturn(JdkFlowAdapter.publisherToFlowPublisher(messages));
    when(mockMessageStream.acknowledgeMessage(any(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
    when(messagesManager.getMessages(ACI, device)).thenReturn(mockMessageStream);
    return mockMessageStream;
  }

  private static Envelope createMessage(final ServiceIdentifier sourceId,
      final UUID destinationUuid,
      final long clientTimestamp,
      final String content) {

    final UUID serverGuid = UUID.randomUUID();
    return Envelope.newBuilder()
        .setServerGuid(UUIDUtil.toByteString(serverGuid))
        .setType(Envelope.Type.CIPHERTEXT)
        .setClientTimestamp(clientTimestamp)
        .setServerTimestamp(0)
        .setSourceServiceId(sourceId.toCompactByteString())
        .setSourceDevice(SOURCE_DEVICE_ID)
        .setDestinationServiceId(new AciServiceIdentifier(destinationUuid).toCompactByteString())
        .setContent(ByteString.copyFrom(content.getBytes(StandardCharsets.UTF_8)))
        .build();
  }
}
