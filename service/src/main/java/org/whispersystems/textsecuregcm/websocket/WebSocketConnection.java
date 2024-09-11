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
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.commons.lang3.StringUtils;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.DisplacedPresenceListener;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessageAvailabilityListener;
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

public class WebSocketConnection implements MessageAvailabilityListener, DisplacedPresenceListener {

  private static final DistributionSummary messageTime = Metrics.summary(
      name(MessageController.class, "messageDeliveryDuration"));
  private static final DistributionSummary primaryDeviceMessageTime = Metrics.summary(
      name(MessageController.class, "primaryDeviceMessageDeliveryDuration"));
  private static final Counter sendMessageCounter = Metrics.counter(name(WebSocketConnection.class, "sendMessage"));
  private static final Counter messageAvailableCounter = Metrics.counter(
      name(WebSocketConnection.class, "messagesAvailable"));
  private static final Counter messagesPersistedCounter = Metrics.counter(
      name(WebSocketConnection.class, "messagesPersisted"));
  private static final Counter bytesSentCounter = Metrics.counter(name(WebSocketConnection.class, "bytesSent"));
  private static final Counter sendFailuresCounter = Metrics.counter(name(WebSocketConnection.class, "sendFailures"));

  private static final String INITIAL_QUEUE_LENGTH_DISTRIBUTION_NAME = name(WebSocketConnection.class,
      "initialQueueLength");
  private static final String INITIAL_QUEUE_DRAIN_TIMER_NAME = name(WebSocketConnection.class, "drainInitialQueue");
  private static final String SLOW_QUEUE_DRAIN_COUNTER_NAME = name(WebSocketConnection.class, "slowQueueDrain");
  private static final String QUEUE_DRAIN_RETRY_COUNTER_NAME = name(WebSocketConnection.class, "queueDrainRetry");
  private static final String DISPLACEMENT_COUNTER_NAME = name(WebSocketConnection.class, "displacement");
  private static final String NON_SUCCESS_RESPONSE_COUNTER_NAME = name(WebSocketConnection.class,
      "clientNonSuccessResponse");
  private static final String CLIENT_CLOSED_MESSAGE_AVAILABLE_COUNTER_NAME = name(WebSocketConnection.class,
      "messageAvailableAfterClientClosed");
  private static final String SEND_MESSAGES_FLUX_NAME = MetricsUtil.name(WebSocketConnection.class,
      "sendMessages");
  private static final String SEND_MESSAGE_ERROR_COUNTER = MetricsUtil.name(WebSocketConnection.class,
      "sendMessageError");
  private static final String STATUS_CODE_TAG = "status";
  private static final String STATUS_MESSAGE_TAG = "message";
  private static final String ERROR_TYPE_TAG = "errorType";

  private static final long SLOW_DRAIN_THRESHOLD = 10_000;

  @VisibleForTesting
  static final int MESSAGE_PUBLISHER_LIMIT_RATE = 100;

  @VisibleForTesting
  static final int MESSAGE_SENDER_MAX_CONCURRENCY = 256;

  @VisibleForTesting
  static final int MAX_CONSECUTIVE_RETRIES = 5;
  private static final long RETRY_DELAY_MILLIS = 1_000;
  private static final int RETRY_DELAY_JITTER_MILLIS = 500;

  private static final int DEFAULT_SEND_FUTURES_TIMEOUT_MILLIS = 5 * 60 * 1000;

  private static final Duration CLOSE_WITH_PENDING_MESSAGES_NOTIFICATION_DELAY = Duration.ofMinutes(1);

  private static final Logger logger = LoggerFactory.getLogger(WebSocketConnection.class);

  private final ReceiptSender receiptSender;
  private final MessagesManager messagesManager;
  private final MessageMetrics messageMetrics;
  private final PushNotificationManager pushNotificationManager;
  private final PushNotificationScheduler pushNotificationScheduler;
  private final MessageDeliveryLoopMonitor messageDeliveryLoopMonitor;

  private final AuthenticatedDevice auth;
  private final WebSocketClient client;

  private final int sendFuturesTimeoutMillis;

  private final ScheduledExecutorService scheduledExecutorService;

  private final Semaphore processStoredMessagesSemaphore = new Semaphore(1);
  private final AtomicReference<StoredMessageState> storedMessageState = new AtomicReference<>(
      StoredMessageState.PERSISTED_NEW_MESSAGES_AVAILABLE);
  private final AtomicBoolean sentInitialQueueEmptyMessage = new AtomicBoolean(false);
  private final LongAdder sentMessageCounter = new LongAdder();
  private final AtomicLong queueDrainStartTime = new AtomicLong();
  private final AtomicInteger consecutiveRetries = new AtomicInteger();
  private final AtomicReference<ScheduledFuture<?>> retryFuture = new AtomicReference<>();
  private final AtomicReference<Disposable> messageSubscription = new AtomicReference<>();

  private final Random random = new Random();
  private final Scheduler messageDeliveryScheduler;

  private final ClientReleaseManager clientReleaseManager;

  private enum StoredMessageState {
    EMPTY,
    CACHED_NEW_MESSAGES_AVAILABLE,
    PERSISTED_NEW_MESSAGES_AVAILABLE
  }

  public WebSocketConnection(ReceiptSender receiptSender,
      MessagesManager messagesManager,
      MessageMetrics messageMetrics,
      PushNotificationManager pushNotificationManager,
      PushNotificationScheduler pushNotificationScheduler,
      AuthenticatedDevice auth,
      WebSocketClient client,
      ScheduledExecutorService scheduledExecutorService,
      Scheduler messageDeliveryScheduler,
      ClientReleaseManager clientReleaseManager,
      MessageDeliveryLoopMonitor messageDeliveryLoopMonitor) {

    this(receiptSender,
        messagesManager,
        messageMetrics,
        pushNotificationManager,
        pushNotificationScheduler,
        auth,
        client,
        DEFAULT_SEND_FUTURES_TIMEOUT_MILLIS,
        scheduledExecutorService,
        messageDeliveryScheduler,
        clientReleaseManager,
        messageDeliveryLoopMonitor);
  }

  @VisibleForTesting
  WebSocketConnection(ReceiptSender receiptSender,
      MessagesManager messagesManager,
      MessageMetrics messageMetrics,
      PushNotificationManager pushNotificationManager,
      PushNotificationScheduler pushNotificationScheduler,
      AuthenticatedDevice auth,
      WebSocketClient client,
      int sendFuturesTimeoutMillis,
      ScheduledExecutorService scheduledExecutorService,
      Scheduler messageDeliveryScheduler,
      ClientReleaseManager clientReleaseManager,
      MessageDeliveryLoopMonitor messageDeliveryLoopMonitor) {

    this.receiptSender = receiptSender;
    this.messagesManager = messagesManager;
    this.messageMetrics = messageMetrics;
    this.pushNotificationManager = pushNotificationManager;
    this.pushNotificationScheduler = pushNotificationScheduler;
    this.auth = auth;
    this.client = client;
    this.sendFuturesTimeoutMillis = sendFuturesTimeoutMillis;
    this.scheduledExecutorService = scheduledExecutorService;
    this.messageDeliveryScheduler = messageDeliveryScheduler;
    this.clientReleaseManager = clientReleaseManager;
    this.messageDeliveryLoopMonitor = messageDeliveryLoopMonitor;
  }

  public void start() {
    pushNotificationManager.handleMessagesRetrieved(auth.getAccount(), auth.getAuthenticatedDevice(), client.getUserAgent());
    queueDrainStartTime.set(System.currentTimeMillis());
    processStoredMessages();
  }

  public void stop() {
    final ScheduledFuture<?> future = retryFuture.get();

    if (future != null) {
      future.cancel(false);
    }

    final Disposable subscription = messageSubscription.get();
    if (subscription != null) {
      subscription.dispose();
    }

    client.close(1000, "OK");

    if (storedMessageState.get() != StoredMessageState.EMPTY) {
      pushNotificationScheduler.scheduleDelayedNotification(auth.getAccount(),
          auth.getAuthenticatedDevice(),
          CLOSE_WITH_PENDING_MESSAGES_NOTIFICATION_DELAY);
    }
  }

  private CompletableFuture<Void> sendMessage(final Envelope message, StoredMessageInfo storedMessageInfo) {
    // clear ephemeral field from the envelope
    final Optional<byte[]> body = Optional.ofNullable(message.toBuilder().clearEphemeral().build().toByteArray());

    sendMessageCounter.increment();
    sentMessageCounter.increment();
    bytesSentCounter.increment(body.map(bytes -> bytes.length).orElse(0));
    messageMetrics.measureAccountEnvelopeUuidMismatches(auth.getAccount(), message);

    // X-Signal-Key: false must be sent until Android stops assuming it missing means true
    return client.sendRequest("PUT", "/api/v1/message",
            List.of(HeaderUtils.X_SIGNAL_KEY + ": false", HeaderUtils.getTimestampHeader()), body)
        .whenComplete((ignored, throwable) -> {
          if (throwable != null) {
            sendFailuresCounter.increment();
          } else {
            messageMetrics.measureOutgoingMessageLatency(message.getServerTimestamp(),
                "websocket",
                auth.getAuthenticatedDevice().isPrimary(),
                client.getUserAgent(),
                clientReleaseManager);
          }
        }).thenCompose(response -> {
          final CompletableFuture<Void> result;
          if (isSuccessResponse(response)) {

            result = messagesManager.delete(auth.getAccount().getUuid(), auth.getAuthenticatedDevice(),
                    storedMessageInfo.guid(), storedMessageInfo.serverTimestamp())
                .thenApply(ignored -> null);

            if (message.getType() != Envelope.Type.SERVER_DELIVERY_RECEIPT) {
              recordMessageDeliveryDuration(message.getServerTimestamp(), auth.getAuthenticatedDevice());
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
        });
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
          auth.getAuthenticatedDevice().getId(), AciServiceIdentifier.valueOf(message.getSourceServiceId()),
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
      final CompletableFuture<Void> queueCleared = new CompletableFuture<>();

      sendMessages(state != StoredMessageState.PERSISTED_NEW_MESSAGES_AVAILABLE, queueCleared);

      setQueueClearedHandler(state, queueCleared);
    }
  }

  private void setQueueClearedHandler(final StoredMessageState state, final CompletableFuture<Void> queueCleared) {

    queueCleared.whenComplete((v, cause) -> {
      if (cause == null) {
        consecutiveRetries.set(0);

        if (sentInitialQueueEmptyMessage.compareAndSet(false, true)) {
          final List<Tag> tags = List.of(
              UserAgentTagUtil.getPlatformTag(client.getUserAgent())
          );
          final long drainDuration = System.currentTimeMillis() - queueDrainStartTime.get();

          Metrics.summary(INITIAL_QUEUE_LENGTH_DISTRIBUTION_NAME, tags).record(sentMessageCounter.sum());
          Metrics.timer(INITIAL_QUEUE_DRAIN_TIMER_NAME, tags).record(drainDuration, TimeUnit.MILLISECONDS);

          if (drainDuration > SLOW_DRAIN_THRESHOLD) {
            Metrics.counter(SLOW_QUEUE_DRAIN_COUNTER_NAME, tags).increment();
          }

          client.sendRequest("PUT", "/api/v1/queue/empty",
              Collections.singletonList(HeaderUtils.getTimestampHeader()), Optional.empty());
        }
      } else {
        storedMessageState.compareAndSet(StoredMessageState.EMPTY, state);
      }

      processStoredMessagesSemaphore.release();

      if (cause == null) {
        if (storedMessageState.get() != StoredMessageState.EMPTY) {
          processStoredMessages();
        }
      } else {
        if (client.isOpen()) {

          if (consecutiveRetries.incrementAndGet() > MAX_CONSECUTIVE_RETRIES) {
            logger.warn("Max consecutive retries exceeded", cause);
            client.close(1011, "Failed to retrieve messages");
          } else {
            logger.debug("Failed to clear queue", cause);
            final List<Tag> tags = List.of(UserAgentTagUtil.getPlatformTag(client.getUserAgent()));

            Metrics.counter(QUEUE_DRAIN_RETRY_COUNTER_NAME, tags).increment();

            final long delay = RETRY_DELAY_MILLIS + random.nextInt(RETRY_DELAY_JITTER_MILLIS);
            retryFuture
                .set(scheduledExecutorService.schedule(this::processStoredMessages, delay, TimeUnit.MILLISECONDS));
          }
        } else {
          logger.debug("Client disconnected before queue cleared");
        }
      }
    });
  }

  private void sendMessages(final boolean cachedMessagesOnly, final CompletableFuture<Void> queueCleared) {

    final Publisher<Envelope> messages =
        messagesManager.getMessagesForDeviceReactive(auth.getAccount().getUuid(), auth.getAuthenticatedDevice(), cachedMessagesOnly);

    final AtomicBoolean hasSentFirstMessage = new AtomicBoolean();
    final AtomicBoolean hasErrored = new AtomicBoolean();

    final Disposable subscription = Flux.from(messages)
        .name(SEND_MESSAGES_FLUX_NAME)
        .tap(Micrometer.metrics(Metrics.globalRegistry))
        .limitRate(MESSAGE_PUBLISHER_LIMIT_RATE)
        .doOnNext(envelope -> {
          if (hasSentFirstMessage.compareAndSet(false, true)) {
            messageDeliveryLoopMonitor.recordDeliveryAttempt(auth.getAccount().getIdentifier(IdentityType.ACI),
                auth.getAuthenticatedDevice().getId(),
                UUID.fromString(envelope.getServerGuid()),
                client.getUserAgent(),
                "websocket");
          }
        })
        .flatMapSequential(envelope ->
            Mono.fromFuture(() -> sendMessage(envelope)
                    .orTimeout(sendFuturesTimeoutMillis, TimeUnit.MILLISECONDS))
                .onErrorResume(
                    // let the first error pass through to terminate the subscription
                    e -> {
                      final boolean firstError = !hasErrored.getAndSet(true);
                      measureSendMessageErrors(e, firstError);

                      return !firstError;
                    },
                    // otherwise just emit nothing
                    e -> Mono.empty()
                ), MESSAGE_SENDER_MAX_CONCURRENCY)
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
  }

  private void measureSendMessageErrors(Throwable e, final boolean terminal) {
    final String errorType;
    if (e instanceof TimeoutException) {
      errorType = "timeout";
    } else if (e instanceof java.nio.channels.ClosedChannelException) {
      errorType = "closedChannel";
    } else if (e == WebSocketResourceProvider.CONNECTION_CLOSED_EXCEPTION) {
      errorType = "connectionClosed";
    } else if (e instanceof org.eclipse.jetty.io.EofException) {
      errorType = "connectionEof";
    } else {
      logger.warn(terminal ? "Send message failure terminated stream" : "Send message failed", e);
      errorType = "other";
    }
    final Tags tags = Tags.of(
        UserAgentTagUtil.getPlatformTag(client.getUserAgent()),
        Tag.of(ERROR_TYPE_TAG, errorType));
    Metrics.counter(SEND_MESSAGE_ERROR_COUNTER, tags).increment();
  }

  private CompletableFuture<Void> sendMessage(Envelope envelope) {
    final UUID messageGuid = UUID.fromString(envelope.getServerGuid());

    if (envelope.getStory() && !client.shouldDeliverStories()) {
      messagesManager.delete(auth.getAccount().getUuid(), auth.getAuthenticatedDevice(), messageGuid, envelope.getServerTimestamp());

      return CompletableFuture.completedFuture(null);
    } else {
      return sendMessage(envelope, new StoredMessageInfo(messageGuid, envelope.getServerTimestamp()));
    }
  }

  @Override
  public boolean handleNewMessagesAvailable() {
    if (!client.isOpen()) {
      // The client may become closed without successful removal of references to the `MessageAvailabilityListener`
      Metrics.counter(CLIENT_CLOSED_MESSAGE_AVAILABLE_COUNTER_NAME).increment();
      return false;
    }

    messageAvailableCounter.increment();

    storedMessageState.compareAndSet(StoredMessageState.EMPTY, StoredMessageState.CACHED_NEW_MESSAGES_AVAILABLE);

    processStoredMessages();

    return true;
  }

  @Override
  public boolean handleMessagesPersisted() {
    if (!client.isOpen()) {
      // The client may become without successful removal of references to the `MessageAvailabilityListener`
      Metrics.counter(CLIENT_CLOSED_MESSAGE_AVAILABLE_COUNTER_NAME).increment();
      return false;
    }
    messagesPersistedCounter.increment();

    storedMessageState.set(StoredMessageState.PERSISTED_NEW_MESSAGES_AVAILABLE);

    processStoredMessages();

    return true;
  }

  @Override
  public void handleDisplacement(final boolean connectedElsewhere) {
    final Tags tags = Tags.of(
        UserAgentTagUtil.getPlatformTag(client.getUserAgent()),
        Tag.of("connectedElsewhere", String.valueOf(connectedElsewhere))
    );

    Metrics.counter(DISPLACEMENT_COUNTER_NAME, tags).increment();

    final int code;
    final String message;

    if (connectedElsewhere) {
      code = 4409;
      message = "Connected elsewhere";
    } else {
      code = 1000;
      message = "OK";
    }

    try {
      client.close(code, message);
    } catch (final Exception e) {
      logger.warn("Orderly close failed", e);

      client.hardDisconnectQuietly();
    }
  }

  private record StoredMessageInfo(UUID guid, long serverTimestamp) {

  }
}
