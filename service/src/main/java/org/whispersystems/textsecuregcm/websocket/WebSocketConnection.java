/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import io.micrometer.core.instrument.Timer;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.util.StaticException;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestListener;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.MessageAvailabilityListener;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.WebSocketResourceProvider;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import reactor.core.Disposable;
import reactor.core.observability.micrometer.Micrometer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;

public class WebSocketConnection implements MessageAvailabilityListener, DisconnectionRequestListener {

  private static final DistributionSummary messageTime = Metrics.summary(
      name(MessageController.class, "messageDeliveryDuration"));
  private static final DistributionSummary primaryDeviceMessageTime = Metrics.summary(
      name(MessageController.class, "primaryDeviceMessageDeliveryDuration"));
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
  private static final String MESSAGE_AVAILABLE_COUNTER_NAME = name(WebSocketConnection.class, "messagesAvailable");
  private static final String MESSAGES_PERSISTED_COUNTER_NAME = name(WebSocketConnection.class, "messagesPersisted");
  private static final String SEND_MESSAGE_DURATION_TIMER_NAME = name(WebSocketConnection.class, "sendMessageDuration");

  private static final String PRESENCE_MANAGER_TAG = "presenceManager";
  private static final String STATUS_CODE_TAG = "status";
  private static final String STATUS_MESSAGE_TAG = "message";
  private static final String ERROR_TYPE_TAG = "errorType";
  private static final String EXCEPTION_TYPE_TAG = "exceptionType";

  private static final long SLOW_DRAIN_THRESHOLD = 10_000;

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
  private final WebSocketClient client;

  private final Semaphore processStoredMessagesSemaphore = new Semaphore(1);
  private final AtomicReference<StoredMessageState> storedMessageState = new AtomicReference<>(
      StoredMessageState.PERSISTED_NEW_MESSAGES_AVAILABLE);
  private final AtomicBoolean sentInitialQueueEmptyMessage = new AtomicBoolean(false);
  private final LongAdder sentMessageCounter = new LongAdder();
  private final AtomicLong queueDrainStartTime = new AtomicLong();
  private final AtomicReference<Disposable> messageSubscription = new AtomicReference<>();

  private final Scheduler messageDeliveryScheduler;

  private final ClientReleaseManager clientReleaseManager;

  private enum StoredMessageState {
    EMPTY,
    CACHED_NEW_MESSAGES_AVAILABLE,
    PERSISTED_NEW_MESSAGES_AVAILABLE
  }

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
  }

  public void start() {
    pushNotificationManager.handleMessagesRetrieved(authenticatedAccount, authenticatedDevice, client.getUserAgent());
    queueDrainStartTime.set(System.currentTimeMillis());
    processStoredMessages();
  }

  public void stop() {
    final Disposable subscription = messageSubscription.get();
    if (subscription != null) {
      subscription.dispose();
    }

    client.close(1000, "OK");

    if (storedMessageState.get() != StoredMessageState.EMPTY) {
      pushNotificationScheduler.scheduleDelayedNotification(authenticatedAccount,
          authenticatedDevice,
          CLOSE_WITH_PENDING_MESSAGES_NOTIFICATION_DELAY);
    }
  }

  private CompletableFuture<Void> sendMessage(final Envelope message, StoredMessageInfo storedMessageInfo) {
    // clear ephemeral field from the envelope
    final Optional<byte[]> body = Optional.ofNullable(message.toBuilder().clearEphemeral().build().toByteArray());

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

            result = messagesManager.delete(authenticatedAccount.getIdentifier(IdentityType.ACI), authenticatedDevice,
                    storedMessageInfo.guid(), storedMessageInfo.serverTimestamp())
                .thenApply(ignored -> null);

            if (message.getType() != Envelope.Type.SERVER_DELIVERY_RECEIPT) {
              recordMessageDeliveryDuration(message.getServerTimestamp(), authenticatedDevice);
              sendDeliveryReceiptFor(message);
            }
          } else {
              final List<Tag> tags = new ArrayList<>(
                  List.of(
                      Tag.of(STATUS_CODE_TAG, String.valueOf(response.getStatus())),
                      UserAgentTagUtil.getPlatformTag(client.getUserAgent())
                  ));

              // TODO Remove this once we've identified the cause of message rejections from desktop clients
              if (StringUtils.isNotBlank(response.getMessage())) {
                tags.add(Tag.of(STATUS_MESSAGE_TAG, response.getMessage()));
              }

              Metrics.counter(NON_SUCCESS_RESPONSE_COUNTER_NAME, tags).increment();

              result = CompletableFuture.completedFuture(null);
            }

          return result;
        })
        .thenRun(() -> sample.stop(Timer.builder(SEND_MESSAGE_DURATION_TIMER_NAME)
            .publishPercentileHistogram(true)
            .tags(Tags.of(UserAgentTagUtil.getPlatformTag(client.getUserAgent())))
            .register(Metrics.globalRegistry)));
  }

  public static void recordMessageDeliveryDuration(long timestamp, Device messageDestinationDevice) {
    final long messageDeliveryDuration = System.currentTimeMillis() - timestamp;
    messageTime.record(messageDeliveryDuration);
    if (messageDestinationDevice.isPrimary()) {
      primaryDeviceMessageTime.record(messageDeliveryDuration);
    }
  }

  private void sendDeliveryReceiptFor(Envelope message) {
    if (!message.hasSourceServiceId()) {
      return;
    }

    try {
      receiptSender.sendReceipt(ServiceIdentifier.valueOf(message.getDestinationServiceId()),
          authenticatedDevice.getId(), AciServiceIdentifier.valueOf(message.getSourceServiceId()),
          message.getClientTimestamp());
    } catch (IllegalArgumentException e) {
      logger.error("Could not parse UUID: {}", message.getSourceServiceId());
    } catch (Exception e) {
      logger.warn("Failed to send receipt", e);
    }
  }

  private boolean isSuccessResponse(WebSocketResponseMessage response) {
    return response != null && response.getStatus() >= 200 && response.getStatus() < 300;
  }

  @VisibleForTesting
  void processStoredMessages() {
    if (processStoredMessagesSemaphore.tryAcquire()) {
      final StoredMessageState state = storedMessageState.getAndSet(StoredMessageState.EMPTY);
      final boolean cachedMessagesOnly = state != StoredMessageState.PERSISTED_NEW_MESSAGES_AVAILABLE;
      sendMessages(cachedMessagesOnly)
          // Update our state with the outcome, send the empty queue message if we need to, and release the semaphore
          .whenComplete((ignored, cause) -> {
            try {
              if (cause != null) {
                // We failed, if the state is currently EMPTY, set it to what it was before we tried
                storedMessageState.compareAndSet(StoredMessageState.EMPTY, state);
                return;
              }

              // Cleared the queue! Send a queue empty message if we need to
              if (sentInitialQueueEmptyMessage.compareAndSet(false, true)) {
                final Tags tags = Tags.of(UserAgentTagUtil.getPlatformTag(client.getUserAgent()));
                final long drainDuration = System.currentTimeMillis() - queueDrainStartTime.get();

                Metrics.summary(INITIAL_QUEUE_LENGTH_DISTRIBUTION_NAME, tags).record(sentMessageCounter.sum());
                Metrics.timer(INITIAL_QUEUE_DRAIN_TIMER_NAME, tags).record(drainDuration, TimeUnit.MILLISECONDS);

                if (drainDuration > SLOW_DRAIN_THRESHOLD) {
                  Metrics.counter(SLOW_QUEUE_DRAIN_COUNTER_NAME, tags).increment();
                }

                client.sendRequest("PUT", "/api/v1/queue/empty",
                    Collections.singletonList(HeaderUtils.getTimestampHeader()), Optional.empty());
              }
            } finally {
              processStoredMessagesSemaphore.release();
            }
          })
          // Potentially kick off more work, must happen after we release the semaphore
          .whenComplete((ignored, cause) -> {
            if (cause != null) {
              if (!client.isOpen()) {
                logger.debug("Client disconnected before queue cleared");
                return;
              }

              client.close(1011, "Failed to retrieve messages");
              return;
            }

            // Success, but check if more messages came in while we were processing
            if (storedMessageState.get() != StoredMessageState.EMPTY) {
              processStoredMessages();
            }
          });
    }
  }

  private CompletableFuture<Void> sendMessages(final boolean cachedMessagesOnly) {
    final CompletableFuture<Void> queueCleared = new CompletableFuture<>();

    final Publisher<Envelope> messages =
        messagesManager.getMessagesForDeviceReactive(authenticatedAccount.getIdentifier(IdentityType.ACI), authenticatedDevice, cachedMessagesOnly);

    final AtomicBoolean hasSentFirstMessage = new AtomicBoolean();

    final Disposable subscription = Flux.from(messages)
        .name(SEND_MESSAGES_FLUX_NAME)
        .tap(Micrometer.metrics(Metrics.globalRegistry))
        .limitRate(MESSAGE_PUBLISHER_LIMIT_RATE)
        .doOnNext(envelope -> {
          if (hasSentFirstMessage.compareAndSet(false, true)) {
            messageDeliveryLoopMonitor.recordDeliveryAttempt(authenticatedAccount.getIdentifier(IdentityType.ACI),
                authenticatedDevice.getId(),
                UUID.fromString(envelope.getServerGuid()),
                client.getUserAgent(),
                "websocket");
          }
        })
        .flatMapSequential(envelope -> Mono.fromFuture(() -> sendMessage(envelope))
                .retryWhen(Retry.backoff(4, Duration.ofSeconds(1)).filter(throwable -> !isConnectionClosedException(throwable))),
            MESSAGE_SENDER_MAX_CONCURRENCY)
        .doOnError(this::measureSendMessageErrors)
        .subscribeOn(messageDeliveryScheduler)
        .subscribe(
            // no additional consumer of values - it is Flux<Void> by now
            null,
            // this first error will terminate the stream, but we may get multiple errors from in-flight messages
            queueCleared::completeExceptionally,
            // completion
            () -> queueCleared.complete(null)
        );

    messageSubscription.set(subscription);
    return queueCleared;
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
            UserAgentTagUtil.getPlatformTag(client.getUserAgent()),
            Tag.of(ERROR_TYPE_TAG, errorType),
            Tag.of(EXCEPTION_TYPE_TAG, e.getClass().getSimpleName())))
        .increment();
  }

  private static boolean isConnectionClosedException(final Throwable throwable) {
    return throwable instanceof java.nio.channels.ClosedChannelException ||
        throwable == WebSocketResourceProvider.CONNECTION_CLOSED_EXCEPTION ||
        throwable instanceof org.eclipse.jetty.io.EofException ||
        (throwable instanceof StaticException staticException && "Closed".equals(staticException.getMessage()));
  }

  private CompletableFuture<Void> sendMessage(Envelope envelope) {
    final UUID messageGuid = UUID.fromString(envelope.getServerGuid());

    if (envelope.getStory() && !client.shouldDeliverStories()) {
      messagesManager.delete(authenticatedAccount.getIdentifier(IdentityType.ACI), authenticatedDevice, messageGuid, envelope.getServerTimestamp());

      return CompletableFuture.completedFuture(null);
    } else {
      return sendMessage(envelope, new StoredMessageInfo(messageGuid, envelope.getServerTimestamp()));
    }
  }

  @Override
  public void handleNewMessageAvailable() {
    Metrics.counter(MESSAGE_AVAILABLE_COUNTER_NAME,
            PRESENCE_MANAGER_TAG, "pubsub")
        .increment();

    storedMessageState.compareAndSet(StoredMessageState.EMPTY, StoredMessageState.CACHED_NEW_MESSAGES_AVAILABLE);

    processStoredMessages();
  }

  @Override
  public void handleMessagesPersisted() {
    Metrics.counter(MESSAGES_PERSISTED_COUNTER_NAME,
            PRESENCE_MANAGER_TAG, "pubsub")
        .increment();

    storedMessageState.set(StoredMessageState.PERSISTED_NEW_MESSAGES_AVAILABLE);

    processStoredMessages();
  }

  @Override
  public void handleConflictingMessageConsumer() {
    closeConnection(4409, "Connected elsewhere");
  }

  @Override
  public void handleDisconnectionRequest() {
    closeConnection(4401, "Reauthentication required");
  }

  private void closeConnection(final int code, final String message) {
    final Tags tags = Tags.of(
        UserAgentTagUtil.getPlatformTag(client.getUserAgent()),
        // TODO We should probably just use the status code directly
        Tag.of("connectedElsewhere", String.valueOf(code == 4409)));

    Metrics.counter(DISPLACEMENT_COUNTER_NAME, tags).increment();

    client.close(code, message);
  }

  private record StoredMessageInfo(UUID guid, long serverTimestamp) {

  }
}
