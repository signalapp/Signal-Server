/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Empty;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import java.util.UUID;
import javax.annotation.Nullable;
import org.signal.chat.messages.GetMessagesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestListener;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
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
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.observability.micrometer.Micrometer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.core.publisher.Sinks;

/// A MessageDispatcher is used to stream messages to an authenticated device and coordinates message acknowledgement,
/// backpressure, disconnection signals, and receipts.
public class MessageDispatcher {

  private static final Logger log = LoggerFactory.getLogger(MessageDispatcher.class);

  @VisibleForTesting
  static final int MAX_UNACKED_MESSAGES = 256;
  @VisibleForTesting
  static final Duration CLOSE_WITH_PENDING_MESSAGES_NOTIFICATION_DELAY = Duration.ofMinutes(1);
  private static final int MESSAGE_PUBLISHER_LIMIT_RATE = 100;

  public static final GetMessagesResponse QUEUE_EMPTY_RESPONSE =
      GetMessagesResponse.newBuilder().setQueueEmpty(Empty.getDefaultInstance()).build();
  private static final GetMessagesResponse CONFLICTING_STREAM_RESPONSE = GetMessagesResponse.newBuilder()
      .setTerminated(GetMessagesResponse.Terminated.newBuilder()
          .setConflictingStream(Empty.getDefaultInstance()))
      .build();
  private static final GetMessagesResponse REAUTHENTICATION_REQUIRED_RESPONSE = GetMessagesResponse.newBuilder()
      .setTerminated(GetMessagesResponse.Terminated.newBuilder()
          .setReauthenticationRequired(Empty.getDefaultInstance()))
      .build();

  private static final String SEND_MESSAGES_FLUX_NAME = MetricsUtil.name(MessageDispatcher.class, "sendMessages");

  private final ReceiptSender receiptSender;
  private final MessagesManager messagesManager;
  private final MessageMetrics messageMetrics;
  private final PushNotificationManager pushNotificationManager;
  private final PushNotificationScheduler pushNotificationScheduler;
  private final ClientReleaseManager clientReleaseManager;
  private final MessageDeliveryLoopMonitor messageDeliveryLoopMonitor;
  private final DisconnectionRequestManager disconnectionRequestManager;

  public MessageDispatcher(final ReceiptSender receiptSender,
      final MessagesManager messagesManager,
      final MessageMetrics messageMetrics,
      final PushNotificationManager pushNotificationManager,
      final PushNotificationScheduler pushNotificationScheduler,
      final MessageDeliveryLoopMonitor messageDeliveryLoopMonitor,
      final DisconnectionRequestManager disconnectionRequestManager,
      final ClientReleaseManager clientReleaseManager) {
    this.receiptSender = receiptSender;
    this.messagesManager = messagesManager;
    this.messageMetrics = messageMetrics;
    this.pushNotificationManager = pushNotificationManager;
    this.pushNotificationScheduler = pushNotificationScheduler;
    this.messageDeliveryLoopMonitor = messageDeliveryLoopMonitor;
    this.disconnectionRequestManager = disconnectionRequestManager;
    this.clientReleaseManager = clientReleaseManager;
  }

  /// Retrieve messages for the device.
  ///
  /// ## Termination
  /// The returned message stream is potentially infinite. It will terminate if the device acknowledgement stream
  /// terminates for any reason. The error from the client will be propagated to the returned flux. The stream may also
  /// terminate with any other error encountered retrieving messages or processing acknowledgements.
  ///
  /// The returned flux will emit a [GetMessagesResponse.Terminated] and then complete if
  /// - A disconnection request (typically indicating a change in authorization credentials) is received.
  /// - A client requests messages for the same account/device while this stream is still active
  ///
  /// When the stream terminates for any reason and the client has messages remaining in their queue, a push
  /// notification is scheduled to encourage the client to wake up and try again after a delay.
  ///
  /// ## Queue Empty
  /// The returned flux will emit a queue empty indicator when the device has successfully drained their waiting queue
  /// and acknowledged all of them. A queue empty response guarantees all messages that were waiting for the client when
  /// they first connected have been delivered and removed from storage, though it's possible the queue empty response
  /// also waits for messages that arrived in the intervening period.
  ///
  /// ## Acknowledgement
  /// When the device acknowledges a message GUID it is removed from storage. If applicable, a delivery receipt is sent
  /// to the original sender of the message.
  ///
  ///
  /// @param shouldDropStories        if true, stories will not be delivered in the response stream (and will be
  ///                                 removed from persistent storage)
  /// @param userAgentString          the userAgent of the requesting device
  /// @param account                  the account to retrieve messages for
  /// @param device                   the device to retrieve messages for
  /// @param acknowledgedMessageGuids A flux that should emit the server guid of messages when they have been
  ///                                 acknowledged by the device
  /// @return The message stream
  public Flux<GetMessagesResponse> getMessages(
      final boolean shouldDropStories,
      @Nullable final String userAgentString,
      final Account account,
      final Device device,
      final Flux<UUID> acknowledgedMessageGuids) {

    final MessageStream messageStream = messagesManager.getMessages(account.getIdentifier(IdentityType.ACI), device);
    @Nullable final UserAgent userAgent = UserAgentUtil.maybeParseUserAgentString(userAgentString);
    final PendingAcknowledgementTracker pendingAcknowledgementTracker = new PendingAcknowledgementTracker(messageMetrics, userAgent);

    pushNotificationManager.handleMessagesRetrieved(account, device, userAgentString);

    final Flux<GetMessagesResponse> messages = JdkFlowAdapter.flowPublisherToFlux(messageStream.getMessages())
        .name(SEND_MESSAGES_FLUX_NAME)
        .tap(Micrometer.metrics(Metrics.globalRegistry))
        .limitRate(MESSAGE_PUBLISHER_LIMIT_RATE)

        // Check the first message we send for message-delivery loops
        .switchOnFirst((firstEntry, flux) -> {
          recordDeliveryAttempt(account, device, userAgentString, firstEntry);
          return flux;
        })
        .flatMapSequential(entry -> switch (entry) {
          case MessageStreamEntry.Envelope(final MessageProtos.Envelope envelope) -> {
            final UUID serverGuid = UUIDUtil.fromByteString(envelope.getServerGuid());
            if (envelope.getStory() && shouldDropStories) {
              // Just immediately delete stories if the device doesn't want them
              yield Mono.fromFuture(() -> messageStream.acknowledgeMessage(serverGuid, envelope.getServerTimestamp()))
                  .then(Mono.empty());
            }

            pendingAcknowledgementTracker.addUnacknowledgedEnvelope(envelope);
            final MessageProtos.Envelope stripped = envelope.toBuilder().clearEphemeral().build();
            messageMetrics.measureAccountEnvelopeUuidMismatches(account, envelope);
            messageMetrics.measureMessageSent(stripped.getSerializedSize());
            yield Mono.just(GetMessagesResponse.newBuilder().setEnvelope(stripped).build());
          }
          case MessageStreamEntry.QueueEmpty _ -> {
            pendingAcknowledgementTracker.markEndOfQueue();
            yield Mono.empty();
          }
        }, MAX_UNACKED_MESSAGES);

    // Only emit envelopes when the permit flux emits a value. Initially it is seeded MAX_UNACKED_MESSAGES permits,
    // after that a permit is emitted every time we receive an ack.
    final Sinks.Many<Boolean> ackPermits = Sinks.many().unicast().onBackpressureBuffer();
    final Flux<Boolean> permits = Flux.range(0, MAX_UNACKED_MESSAGES)
        .map(_ -> true)
        .concatWith(ackPermits.asFlux());
    final Mono<GetMessagesResponse> ackCompletions = acknowledgedMessageGuids
        .flatMap(guid -> {
          final PendingAcknowledgementTracker.UnacknowledgedEnvelope unacked = pendingAcknowledgementTracker.takeUnacknowledgedEnvelope(guid);
          if (unacked == null) {
            // This is fine, the client may have sent a duplicate-ack
            return Mono.empty();
          }
          messageMetrics.measureOutgoingMessageLatency(
              unacked.getServerTimestamp(), MessageMetrics.GRPC_CHANNEL, device.isPrimary(),
              unacked.isUrgent(), unacked.isEphemeral(), userAgent, clientReleaseManager);

          maybeSendDeliveryReceipt(device, unacked);

          return Mono.fromFuture(() -> messageStream.acknowledgeMessage(unacked.getServerGuid(), unacked.getServerTimestamp()))
              .doOnSuccess(_ -> unacked.handleMessageAcknowledged())
              // Just have to emit some value that indicates we can release a permit
              .thenReturn(true);
        }, MAX_UNACKED_MESSAGES)
        .doOnNext(_ -> ackPermits.tryEmitNext(true))
        .ignoreElements()
        .cast(GetMessagesResponse.class);

    return Flux.merge(
            // Emit messages, but only when a permit is available
            messages.zipWith(permits, (resp, _) -> resp),
            // Will only emit errors, but those should terminate the stream
            ackCompletions,
            // Emits a queue empty once all messages prior to the queueEmpty signal have been acked
            pendingAcknowledgementTracker.queueDrained(),
            // Emit a Terminated response if we receive a disconnection request
            disconnectionSignal(account, device))
        .onErrorResume(ConflictingMessageConsumerException.class, _ -> Mono.just(CONFLICTING_STREAM_RESPONSE))
        .takeUntil(response -> switch (response.getResponseCase()) {
          case ENVELOPE, QUEUE_EMPTY -> false;
          case TERMINATED -> {
            messageMetrics.measureMessageStreamDisplaced(MessageMetrics.GRPC_CHANNEL, userAgent,
                response.getTerminated().hasConflictingStream());
            yield true;
          }
          case RESPONSE_NOT_SET -> throw new IllegalStateException("tried to return an invalid GetMessagesResponse");
        })
        .doFinally(_ -> maybeSchedulePush(account, device));
  }

  /// If the device potentially has more messages available, schedule a push notification.
  private void maybeSchedulePush(final Account account, final Device device) {
    messagesManager.mayHaveMessages(account.getIdentifier(IdentityType.ACI), device)
        .thenAccept(mayHaveMessages -> {
          if (mayHaveMessages) {
            pushNotificationScheduler.scheduleDelayedNotification(account, device,
                CLOSE_WITH_PENDING_MESSAGES_NOTIFICATION_DELAY);
          }
        });
  }

  /// Watch for a disconnection signal from the [DisconnectionRequestManager]
  ///
  /// @return Mono that completes with a [org.signal.chat.messages.GetMessagesResponse.Terminated] response if the
  /// device receives a disconnection request
  private Mono<GetMessagesResponse> disconnectionSignal(final Account account, final Device device) {
    return Mono.create(sink -> {
      final DisconnectionRequestListener listener = () -> sink.success(REAUTHENTICATION_REQUIRED_RESPONSE);
      disconnectionRequestManager.addListener(account.getUuid(), device.getId(), listener);
      sink.onDispose(() -> disconnectionRequestManager.removeListener(account.getUuid(), device.getId(), listener));
    });
  }

  private void recordDeliveryAttempt(final Account account, final Device device, final String userAgent,
      final Signal<? extends MessageStreamEntry> firstEntry) {
    if (firstEntry.get() instanceof MessageStreamEntry.Envelope(final MessageProtos.Envelope message)) {
      messageDeliveryLoopMonitor.recordDeliveryAttempt(account.getIdentifier(IdentityType.ACI),
          device.getId(),
          UUIDUtil.fromByteString(message.getServerGuid()),
          userAgent,
          MessageMetrics.GRPC_CHANNEL);
    }
  }

  private void maybeSendDeliveryReceipt(
      final Device device,
      final PendingAcknowledgementTracker.UnacknowledgedEnvelope unacknowledgedEnvelope) {
    if (unacknowledgedEnvelope.getSourceServiceId() == null) {
      return;
    }

    try {
      receiptSender.sendReceipt(
          unacknowledgedEnvelope.getDestinationServiceId(),
          device.getId(),
          unacknowledgedEnvelope.getSourceServiceId(),
          unacknowledgedEnvelope.getClientTimestamp());
    } catch (final Exception e) {
      log.warn("Failed to send receipt", e);
    }
  }
}
