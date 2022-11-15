/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
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
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.DisplacedPresenceListener;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessageAvailabilityListener;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class WebSocketConnection implements MessageAvailabilityListener, DisplacedPresenceListener {

  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Histogram messageTime = metricRegistry.histogram(
      name(MessageController.class, "message_delivery_duration"));
  private static final Histogram primaryDeviceMessageTime = metricRegistry.histogram(
      name(MessageController.class, "primary_device_message_delivery_duration"));
  private static final Meter sendMessageMeter = metricRegistry.meter(name(WebSocketConnection.class, "send_message"));
  private static final Meter messageAvailableMeter = metricRegistry.meter(
      name(WebSocketConnection.class, "messagesAvailable"));
  private static final Meter messagesPersistedMeter = metricRegistry.meter(
      name(WebSocketConnection.class, "messagesPersisted"));
  private static final Meter bytesSentMeter = metricRegistry.meter(name(WebSocketConnection.class, "bytes_sent"));
  private static final Meter sendFailuresMeter = metricRegistry.meter(name(WebSocketConnection.class, "send_failures"));

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
  static final int MAX_CONSECUTIVE_RETRIES = 5;
  private static final long RETRY_DELAY_MILLIS = 1_000;
  private static final int RETRY_DELAY_JITTER_MILLIS = 500;

  private static final int DEFAULT_SEND_FUTURES_TIMEOUT_MILLIS = 5 * 60 * 1000;

  private static final Logger logger = LoggerFactory.getLogger(WebSocketConnection.class);

  private final ReceiptSender receiptSender;
  private final MessagesManager messagesManager;

  private final AuthenticatedAccount auth;
  private final Device device;
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
  private final Scheduler reactiveScheduler;

  private enum StoredMessageState {
    EMPTY,
    CACHED_NEW_MESSAGES_AVAILABLE,
    PERSISTED_NEW_MESSAGES_AVAILABLE
  }

  public WebSocketConnection(ReceiptSender receiptSender,
      MessagesManager messagesManager,
      AuthenticatedAccount auth,
      Device device,
      WebSocketClient client,
      ScheduledExecutorService scheduledExecutorService) {

    this(receiptSender,
        messagesManager,
        auth,
        device,
        client,
        scheduledExecutorService,
        Schedulers.boundedElastic());
  }

  @VisibleForTesting
  WebSocketConnection(ReceiptSender receiptSender,
      MessagesManager messagesManager,
      AuthenticatedAccount auth,
      Device device,
      WebSocketClient client,
      ScheduledExecutorService scheduledExecutorService,
      Scheduler reactiveScheduler) {

    this(receiptSender,
        messagesManager,
        auth,
        device,
        client,
        DEFAULT_SEND_FUTURES_TIMEOUT_MILLIS,
        scheduledExecutorService,
        reactiveScheduler);
  }

  @VisibleForTesting
  WebSocketConnection(ReceiptSender receiptSender,
      MessagesManager messagesManager,
      AuthenticatedAccount auth,
      Device device,
      WebSocketClient client,
      int sendFuturesTimeoutMillis,
      ScheduledExecutorService scheduledExecutorService,
      Scheduler reactiveScheduler) {

    this.receiptSender = receiptSender;
    this.messagesManager = messagesManager;
    this.auth = auth;
    this.device = device;
    this.client = client;
    this.sendFuturesTimeoutMillis = sendFuturesTimeoutMillis;
    this.scheduledExecutorService = scheduledExecutorService;
    this.reactiveScheduler = reactiveScheduler;
  }

  public void start() {
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
  }

  private CompletableFuture<?> sendMessage(final Envelope message, StoredMessageInfo storedMessageInfo) {
    // clear ephemeral field from the envelope
    final Optional<byte[]> body = Optional.ofNullable(message.toBuilder().clearEphemeral().build().toByteArray());

    sendMessageMeter.mark();
    sentMessageCounter.increment();
    bytesSentMeter.mark(body.map(bytes -> bytes.length).orElse(0));
    MessageMetrics.measureAccountEnvelopeUuidMismatches(auth.getAccount(), message);

    // X-Signal-Key: false must be sent until Android stops assuming it missing means true
    return client.sendRequest("PUT", "/api/v1/message",
            List.of(HeaderUtils.X_SIGNAL_KEY + ": false", HeaderUtils.getTimestampHeader()), body)
        .whenComplete((ignored, throwable) -> {
          if (throwable != null) {
            sendFailuresMeter.mark();
          }
        }).thenCompose(response -> {
          final CompletableFuture<?> result;
          if (isSuccessResponse(response)) {

            result = messagesManager.delete(auth.getAccount().getUuid(), device.getId(),
                storedMessageInfo.guid(), storedMessageInfo.serverTimestamp());

            if (message.getType() != Envelope.Type.SERVER_DELIVERY_RECEIPT) {
              recordMessageDeliveryDuration(message.getTimestamp(), device);
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
    messageTime.update(messageDeliveryDuration);
    if (messageDestinationDevice.isMaster()) {
      primaryDeviceMessageTime.update(messageDeliveryDuration);
    }
  }

  private void sendDeliveryReceiptFor(Envelope message) {
    if (!message.hasSourceUuid()) {
      return;
    }

    try {
      receiptSender.sendReceipt(UUID.fromString(message.getDestinationUuid()),
          auth.getAuthenticatedDevice().getId(), UUID.fromString(message.getSourceUuid()),
          message.getTimestamp());
    } catch (IllegalArgumentException e) {
      logger.error("Could not parse UUID: {}", message.getSourceUuid());
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
        messagesManager.getMessagesForDeviceReactive(auth.getAccount().getUuid(), device.getId(), cachedMessagesOnly);

    final Disposable subscription = Flux.from(messages)
        .name(SEND_MESSAGES_FLUX_NAME)
        .metrics()
        .limitRate(MESSAGE_PUBLISHER_LIMIT_RATE)
        .flatMapSequential(envelope ->
            Mono.fromFuture(sendMessage(envelope)
                    .orTimeout(sendFuturesTimeoutMillis, TimeUnit.MILLISECONDS))
                .doOnError(e -> {
                  final String errorType;
                  if (!(e instanceof TimeoutException)) {
                    // TimeoutExceptions are expected, no need to log
                    logger.warn("Send message failed", e);
                    errorType = "other";
                  } else {
                    errorType = "timeout";
                  }
                  final Tags tags = Tags.of(
                      UserAgentTagUtil.getPlatformTag(client.getUserAgent()),
                      Tag.of(ERROR_TYPE_TAG, errorType));
                  Metrics.counter(SEND_MESSAGE_ERROR_COUNTER, tags).increment();
                }))
        .doOnError(queueCleared::completeExceptionally)
        .doOnComplete(() -> queueCleared.complete(null))
        .subscribeOn(reactiveScheduler)
        .subscribe();

    messageSubscription.set(subscription);
  }

  private CompletableFuture<?> sendMessage(Envelope envelope) {
    final UUID messageGuid = UUID.fromString(envelope.getServerGuid());

    if (envelope.getStory() && !client.shouldDeliverStories()) {
      messagesManager.delete(auth.getAccount().getUuid(), device.getId(), messageGuid, envelope.getServerTimestamp());

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

    messageAvailableMeter.mark();

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
    messagesPersistedMeter.mark();

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
