/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.util.StaticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestListener;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
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
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.WebSocketResourceProvider;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.Disposable;
import reactor.core.observability.micrometer.Micrometer;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class WebSocketConnection implements DisconnectionRequestListener {

  private static final Counter sendFailuresCounter = Metrics.counter(name(WebSocketConnection.class, "sendFailures"));

  private static final String NON_SUCCESS_RESPONSE_COUNTER_NAME = name(WebSocketConnection.class,
      "clientNonSuccessResponse");
  private static final String SEND_MESSAGES_FLUX_NAME = MetricsUtil.name(WebSocketConnection.class,
      "sendMessages");
  private static final String SEND_MESSAGE_ERROR_COUNTER = MetricsUtil.name(WebSocketConnection.class,
      "sendMessageError");

  private static final String STATUS_CODE_TAG = "status";
  private static final String STATUS_MESSAGE_TAG = "message";
  private static final String ERROR_TYPE_TAG = "errorType";
  private static final String EXCEPTION_TYPE_TAG = "exceptionType";

  @VisibleForTesting
  static final int MESSAGE_PUBLISHER_LIMIT_RATE = 100;

  @VisibleForTesting
  static final int MESSAGE_SENDER_MAX_CONCURRENCY = 256;

  private static final Duration CLOSE_WITH_PENDING_MESSAGES_NOTIFICATION_DELAY = Duration.ofMinutes(1);

  private static final Logger logger = LoggerFactory.getLogger(WebSocketConnection.class);

  private final ReceiptSender receiptSender;
  private final MessagesManager messagesManager;
  private final MessageMetrics messageMetrics;
  private final PushNotificationManager pushNotificationManager;
  private final PushNotificationScheduler pushNotificationScheduler;
  private final MessageDeliveryLoopMonitor messageDeliveryLoopMonitor;
  private final ExperimentEnrollmentManager experimentEnrollmentManager;

  private final Account authenticatedAccount;
  private final Device authenticatedDevice;
  private final MessageStream messageStream;
  private final WebSocketClient client;
  private final @Nullable UserAgent userAgent;

  private final LongAdder sentMessageCounter = new LongAdder();
  private final AtomicReference<Disposable> messageSubscription = new AtomicReference<>();

  private final Scheduler messageDeliveryScheduler;

  private final ClientReleaseManager clientReleaseManager;

  public WebSocketConnection(
      final ReceiptSender receiptSender,
      final MessagesManager messagesManager,
      final MessageMetrics messageMetrics,
      final PushNotificationManager pushNotificationManager,
      final PushNotificationScheduler pushNotificationScheduler,
      final Account authenticatedAccount,
      final Device authenticatedDevice,
      final WebSocketClient client,
      final Scheduler messageDeliveryScheduler,
      final ClientReleaseManager clientReleaseManager,
      final MessageDeliveryLoopMonitor messageDeliveryLoopMonitor,
      final ExperimentEnrollmentManager experimentEnrollmentManager) {

    this.receiptSender = receiptSender;
    this.messagesManager = messagesManager;
    this.messageMetrics = messageMetrics;
    this.pushNotificationManager = pushNotificationManager;
    this.pushNotificationScheduler = pushNotificationScheduler;
    this.authenticatedAccount = authenticatedAccount;
    this.authenticatedDevice = authenticatedDevice;
    this.client = client;
    this.messageDeliveryScheduler = messageDeliveryScheduler;
    this.clientReleaseManager = clientReleaseManager;
    this.messageDeliveryLoopMonitor = messageDeliveryLoopMonitor;
    this.experimentEnrollmentManager = experimentEnrollmentManager;

    this.messageStream =
        messagesManager.getMessages(authenticatedAccount.getIdentifier(IdentityType.ACI), authenticatedDevice);

    this.userAgent = UserAgentUtil.maybeParseUserAgentString(client.getUserAgent());
  }

  public void start() {
    pushNotificationManager.handleMessagesRetrieved(authenticatedAccount, authenticatedDevice, client.getUserAgent());

    final Timer.Sample queueDrainStart = Timer.start();
    final AtomicBoolean hasSentFirstMessage = new AtomicBoolean();

    messageSubscription.set(JdkFlowAdapter.flowPublisherToFlux(messageStream.getMessages())
        .name(SEND_MESSAGES_FLUX_NAME)
        .tap(Micrometer.metrics(Metrics.globalRegistry))
        .limitRate(MESSAGE_PUBLISHER_LIMIT_RATE)
        // We want to handle conflicting connections as soon as possible, and so do this before we start processing
        // messages in the `flatMapSequential` stage below. If we didn't do this first, then we'd wait for clients to
        // process messages before sending the "connected elsewhere" signal, and while that's ultimately not harmful,
        // it's also not ideal.
        .doOnError(ConflictingMessageConsumerException.class, _ -> {
          messageMetrics.measureMessageStreamDisplaced(MessageMetrics.WEBSOCKET_CHANNEL, userAgent, true);
          client.close(4409, "Connected elsewhere");
        })
        .doOnNext(entry -> {
          if (entry instanceof MessageStreamEntry.Envelope(final Envelope message)) {
            if (hasSentFirstMessage.compareAndSet(false, true)) {
              messageDeliveryLoopMonitor.recordDeliveryAttempt(authenticatedAccount.getIdentifier(IdentityType.ACI),
                  authenticatedDevice.getId(),
                  UUIDUtil.fromByteString(message.getServerGuid()),
                  client.getUserAgent(),
                  MessageMetrics.WEBSOCKET_CHANNEL);
            }
          }
        })
        .flatMapSequential(entry -> switch (entry) {
          case MessageStreamEntry.Envelope envelope -> Mono.fromFuture(() -> sendMessage(envelope.message())).thenReturn(entry);
          case MessageStreamEntry.QueueEmpty _ -> Mono.just(entry);
        }, MESSAGE_SENDER_MAX_CONCURRENCY)
        .subscribeOn(messageDeliveryScheduler)
        .subscribe(
            entry -> {
              if (entry instanceof MessageStreamEntry.QueueEmpty) {
                messageMetrics.measureQueueDrain(MessageMetrics.WEBSOCKET_CHANNEL, userAgent, sentMessageCounter.sum(), queueDrainStart);
                client.sendRequest("PUT", "/api/v1/queue/empty",
                    Collections.singletonList(HeaderUtils.getTimestampHeader()), Optional.empty());
              }
            },
            throwable -> {
              // `ConflictingMessageConsumerException` is handled before processing messages
              if (throwable instanceof ConflictingMessageConsumerException) {
                return;
              }

              measureSendMessageErrors(throwable);

              if (!client.isOpen()) {
                logger.debug("Client disconnected before queue cleared");
                return;
              }

              client.close(1011, "Failed to retrieve messages");
            }
        ));
  }

  public void stop() {
    final Disposable subscription = messageSubscription.get();
    if (subscription != null) {
      subscription.dispose();
    }

    client.close(1000, "OK");

    messagesManager.mayHaveMessages(authenticatedAccount.getIdentifier(IdentityType.ACI), authenticatedDevice)
        .thenAccept(mayHaveMessages -> {
          if (mayHaveMessages) {
            pushNotificationScheduler.scheduleDelayedNotification(authenticatedAccount,
                authenticatedDevice,
                CLOSE_WITH_PENDING_MESSAGES_NOTIFICATION_DELAY);
          }
        });
  }

  private CompletableFuture<Void> sendMessage(final Envelope message) {
    if (message.getStory() && !client.shouldDeliverStories()) {
      return messageStream.acknowledgeMessage(UUIDUtil.fromByteString(message.getServerGuid()), message.getServerTimestamp());
    }

    final Optional<byte[]> body = Optional.of(serializeMessage(message));

    sentMessageCounter.increment();
    messageMetrics.measureAccountEnvelopeUuidMismatches(authenticatedAccount, message);
    messageMetrics.measureMessageSent(body.map(bytes -> bytes.length).orElse(0));

    // Retain only the parts of the message we need to avoid retaining the whole `Envelope` in memory longer than
    // necessary
    final UUID messageGuid = UUIDUtil.fromByteString(message.getServerGuid());
    final long serverTimestamp = message.getServerTimestamp();
    final long clientTimestamp = message.getClientTimestamp();
    final boolean isUrgent = message.getUrgent();
    final boolean isEphemeral = message.getEphemeral();
    final ServiceIdentifier destinationServiceIdentifier =
        ServiceIdentifier.fromByteString(message.getDestinationServiceId());

    @Nullable final AciServiceIdentifier sourceServiceIdentifier;
    final boolean shouldSendDeliveryReceipt;

    if (message.hasSourceServiceId()) {
      sourceServiceIdentifier = AciServiceIdentifier.fromByteString(message.getSourceServiceId());
      shouldSendDeliveryReceipt = message.getType() != Envelope.Type.SERVER_DELIVERY_RECEIPT;
    } else {
      sourceServiceIdentifier = null;
      shouldSendDeliveryReceipt = false;
    }

    final Timer.Sample sample = Timer.start();

    return client.sendRequest("PUT", "/api/v1/message",
            List.of(HeaderUtils.getTimestampHeader()), body)
        .whenComplete((ignored, throwable) -> {
          if (throwable != null) {
            sendFailuresCounter.increment();
          } else {
            messageMetrics.measureOutgoingMessageLatency(serverTimestamp,
                MessageMetrics.WEBSOCKET_CHANNEL,
                authenticatedDevice.isPrimary(),
                isUrgent,
                isEphemeral,
                userAgent,
                clientReleaseManager);
          }
        }).thenCompose(response -> {
          final CompletableFuture<Void> result;
          if (isSuccessResponse(response)) {

            result = messageStream.acknowledgeMessage(messageGuid, serverTimestamp);

            if (shouldSendDeliveryReceipt) {
              try {
                receiptSender.sendReceipt(destinationServiceIdentifier,
                    authenticatedDevice.getId(),
                    sourceServiceIdentifier,
                    clientTimestamp);
              } catch (final Exception e) {
                logger.warn("Failed to send receipt", e);
              }
            }
          } else {
            Tags tags = Tags.of(UserAgentTagUtil.getPlatformTag(userAgent), Tag.of(STATUS_CODE_TAG, String.valueOf(response.getStatus())));

            // TODO Remove this once we've identified the cause of message rejections from desktop clients
            if (StringUtils.isNotBlank(response.getMessage())) {
              tags = tags.and(Tag.of(STATUS_MESSAGE_TAG, response.getMessage()));
            }

            Metrics.counter(NON_SUCCESS_RESPONSE_COUNTER_NAME, tags).increment();

            result = CompletableFuture.completedFuture(null);
          }

          return result;
        })
        .thenRun(() -> messageMetrics.measureSendMessageDuration(MessageMetrics.WEBSOCKET_CHANNEL, userAgent, sample));
  }

  @VisibleForTesting
  static byte[] serializeMessage(final Envelope message) {
    return message.toBuilder().clearEphemeral().build().toByteArray();
  }

  private static boolean isSuccessResponse(final WebSocketResponseMessage response) {
    return response != null && response.getStatus() >= 200 && response.getStatus() < 300;
  }

  private void measureSendMessageErrors(final Throwable e) {
    final String errorType;

    if (e instanceof TimeoutException) {
      errorType = "timeout";
    } else if (isConnectionClosedException(e)) {
      errorType = "connectionClosed";
    } else {
      logger.warn("Send message failed", e);
      errorType = "other";
    }

    Metrics.counter(SEND_MESSAGE_ERROR_COUNTER, Tags.of(
            UserAgentTagUtil.getPlatformTag(userAgent),
            Tag.of(ERROR_TYPE_TAG, errorType),
            Tag.of(EXCEPTION_TYPE_TAG, e.getClass().getSimpleName())))
        .increment();
  }

  @VisibleForTesting
  static boolean isConnectionClosedException(final Throwable throwable) {
    return throwable instanceof java.nio.channels.ClosedChannelException ||
        throwable == WebSocketResourceProvider.CONNECTION_CLOSED_EXCEPTION ||
        throwable instanceof org.eclipse.jetty.io.EofException ||
        (throwable instanceof StaticException staticException && "Closed".equals(staticException.getMessage()));
  }

  @Override
  public void handleDisconnectionRequest() {
    messageMetrics.measureMessageStreamDisplaced(MessageMetrics.WEBSOCKET_CHANNEL, userAgent, false);
    client.close(4401, "Reauthentication required");
  }
}
