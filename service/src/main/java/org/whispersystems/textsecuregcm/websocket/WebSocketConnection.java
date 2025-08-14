/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
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
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.WebSocketResourceProvider;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.Disposable;
import reactor.core.observability.micrometer.Micrometer;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class WebSocketConnection implements DisconnectionRequestListener {

  private static final Counter sendMessageCounter = Metrics.counter(name(WebSocketConnection.class, "sendMessage"));
  private static final Counter bytesSentCounter = Metrics.counter(name(WebSocketConnection.class, "bytesSent"));
  private static final Counter sendFailuresCounter = Metrics.counter(name(WebSocketConnection.class, "sendFailures"));

  private static final String INITIAL_QUEUE_LENGTH_DISTRIBUTION_NAME = name(WebSocketConnection.class,
      "initialQueueLength");
  private static final String INITIAL_QUEUE_DRAIN_TIMER_NAME = name(WebSocketConnection.class, "drainInitialQueue");
  private static final String SLOW_QUEUE_DRAIN_COUNTER_NAME = name(WebSocketConnection.class, "slowQueueDrain");
  private static final String DISPLACEMENT_COUNTER_NAME = name(WebSocketConnection.class, "displacement");
  private static final String NON_SUCCESS_RESPONSE_COUNTER_NAME = name(WebSocketConnection.class,
      "clientNonSuccessResponse");
  private static final String SEND_MESSAGES_FLUX_NAME = MetricsUtil.name(WebSocketConnection.class,
      "sendMessages");
  private static final String SEND_MESSAGE_ERROR_COUNTER = MetricsUtil.name(WebSocketConnection.class,
      "sendMessageError");
  private static final String SEND_MESSAGE_DURATION_TIMER_NAME = name(WebSocketConnection.class, "sendMessageDuration");

  private static final String STATUS_CODE_TAG = "status";
  private static final String STATUS_MESSAGE_TAG = "message";
  private static final String ERROR_TYPE_TAG = "errorType";
  private static final String EXCEPTION_TYPE_TAG = "exceptionType";
  private static final String CONNECTED_ELSEWHERE_TAG = "connectedElsewhere";

  private static final Duration SLOW_DRAIN_THRESHOLD = Duration.ofSeconds(10);

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
  private final Tags platformTag;

  private final LongAdder sentMessageCounter = new LongAdder();
  private final AtomicReference<Disposable> messageSubscription = new AtomicReference<>();

  private final Scheduler messageDeliveryScheduler;

  private final ClientReleaseManager clientReleaseManager;

  public WebSocketConnection(final ReceiptSender receiptSender,
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

    this.platformTag = Tags.of(UserAgentTagUtil.getPlatformTag(client.getUserAgent()));
  }

  public void start() {
    pushNotificationManager.handleMessagesRetrieved(authenticatedAccount, authenticatedDevice, client.getUserAgent());

    final long queueDrainStartNanos = System.nanoTime();
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
          Metrics.counter(DISPLACEMENT_COUNTER_NAME, platformTag.and(CONNECTED_ELSEWHERE_TAG, "true")).increment();
          client.close(4409, "Connected elsewhere");
        })
        .doOnNext(entry -> {
          if (entry instanceof MessageStreamEntry.Envelope(final Envelope message)) {
            if (hasSentFirstMessage.compareAndSet(false, true)) {
              messageDeliveryLoopMonitor.recordDeliveryAttempt(authenticatedAccount.getIdentifier(IdentityType.ACI),
                  authenticatedDevice.getId(),
                  UUID.fromString(message.getServerGuid()),
                  client.getUserAgent(),
                  "websocket");
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
                final Duration drainDuration = Duration.ofNanos(System.nanoTime() - queueDrainStartNanos);

                Metrics.summary(INITIAL_QUEUE_LENGTH_DISTRIBUTION_NAME, platformTag).record(sentMessageCounter.sum());
                Metrics.timer(INITIAL_QUEUE_DRAIN_TIMER_NAME, platformTag).record(drainDuration);

                if (drainDuration.compareTo(SLOW_DRAIN_THRESHOLD) > 0) {
                  Metrics.counter(SLOW_QUEUE_DRAIN_COUNTER_NAME, platformTag).increment();
                }

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
      return messageStream.acknowledgeMessage(message);
    }

    final Optional<byte[]> body = Optional.of(serializeMessage(message));

    sendMessageCounter.increment();
    sentMessageCounter.increment();
    bytesSentCounter.increment(body.map(bytes -> bytes.length).orElse(0));
    messageMetrics.measureAccountEnvelopeUuidMismatches(authenticatedAccount, message);

    final Timer.Sample sample = Timer.start();

    // X-Signal-Key: false must be sent until Android stops assuming it missing means true
    return client.sendRequest("PUT", "/api/v1/message",
            List.of(HeaderUtils.X_SIGNAL_KEY + ": false", HeaderUtils.getTimestampHeader()), body)
        .whenComplete((ignored, throwable) -> {
          if (throwable != null) {
            sendFailuresCounter.increment();
          } else {
            messageMetrics.measureOutgoingMessageLatency(message.getServerTimestamp(),
                "websocket",
                authenticatedDevice.isPrimary(),
                message.getUrgent(),
                message.getEphemeral(),
                client.getUserAgent(),
                clientReleaseManager);
          }
        }).thenCompose(response -> {
          final CompletableFuture<Void> result;
          if (isSuccessResponse(response)) {

            result = messageStream.acknowledgeMessage(message);

            if (message.getType() != Envelope.Type.SERVER_DELIVERY_RECEIPT) {
              sendDeliveryReceiptFor(message);
            }
          } else {
            Tags tags = platformTag.and(STATUS_CODE_TAG, String.valueOf(response.getStatus()));

            // TODO Remove this once we've identified the cause of message rejections from desktop clients
            if (StringUtils.isNotBlank(response.getMessage())) {
              tags = tags.and(Tag.of(STATUS_MESSAGE_TAG, response.getMessage()));
            }

            Metrics.counter(NON_SUCCESS_RESPONSE_COUNTER_NAME, tags).increment();

            result = CompletableFuture.completedFuture(null);
          }

          return result;
        })
        .thenRun(() -> sample.stop(Timer.builder(SEND_MESSAGE_DURATION_TIMER_NAME)
            .publishPercentileHistogram(true)
            .tags(platformTag)
            .register(Metrics.globalRegistry)));
  }

  @VisibleForTesting
  static byte[] serializeMessage(final Envelope message) {
    return message.toBuilder().clearEphemeral().build().toByteArray();
  }

  private void sendDeliveryReceiptFor(Envelope message) {
    if (!message.hasSourceServiceId()) {
      return;
    }

    try {
      receiptSender.sendReceipt(ServiceIdentifier.valueOf(message.getDestinationServiceId()),
          authenticatedDevice.getId(), AciServiceIdentifier.valueOf(message.getSourceServiceId()),
          message.getClientTimestamp());
    } catch (final IllegalArgumentException e) {
      logger.error("Could not parse UUID: {}", message.getSourceServiceId());
    } catch (final Exception e) {
      logger.warn("Failed to send receipt", e);
    }
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

    Metrics.counter(SEND_MESSAGE_ERROR_COUNTER,
            platformTag.and(ERROR_TYPE_TAG, errorType, EXCEPTION_TYPE_TAG, e.getClass().getSimpleName()))
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
    Metrics.counter(DISPLACEMENT_COUNTER_NAME, platformTag.and(CONNECTED_ELSEWHERE_TAG, "false")).increment();
    client.close(4401, "Reauthentication required");
  }
}
