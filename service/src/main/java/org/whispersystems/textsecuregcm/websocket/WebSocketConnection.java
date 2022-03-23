/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static com.codahale.metrics.MetricRegistry.name;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import javax.ws.rs.WebApplicationException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.controllers.NoSuchUserException;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.DisplacedPresenceListener;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessageAvailabilityListener;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.TimestampHeaderUtil;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class WebSocketConnection implements MessageAvailabilityListener, DisplacedPresenceListener {

  private static final MetricRegistry metricRegistry                 = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Histogram      messageTime                    = metricRegistry.histogram(name(MessageController.class, "message_delivery_duration"));
  private static final Histogram      primaryDeviceMessageTime       = metricRegistry.histogram(name(MessageController.class, "primary_device_message_delivery_duration"));
  private static final Meter          sendMessageMeter               = metricRegistry.meter(name(WebSocketConnection.class, "send_message"));
  private static final Meter          messageAvailableMeter          = metricRegistry.meter(name(WebSocketConnection.class, "messagesAvailable"));
  private static final Meter          messagesPersistedMeter         = metricRegistry.meter(name(WebSocketConnection.class, "messagesPersisted"));
  private static final Meter          bytesSentMeter                 = metricRegistry.meter(name(WebSocketConnection.class, "bytes_sent"));
  private static final Meter          sendFailuresMeter              = metricRegistry.meter(name(WebSocketConnection.class, "send_failures"));
  private static final Meter          discardedMessagesMeter         = metricRegistry.meter(name(WebSocketConnection.class, "discardedMessages"));

  private static final String INITIAL_QUEUE_LENGTH_DISTRIBUTION_NAME = name(WebSocketConnection.class, "initialQueueLength");
  private static final String INITIAL_QUEUE_DRAIN_TIMER_NAME         = name(WebSocketConnection.class, "drainInitialQueue");
  private static final String SLOW_QUEUE_DRAIN_COUNTER_NAME          = name(WebSocketConnection.class, "slowQueueDrain");
  private static final String QUEUE_DRAIN_RETRY_COUNTER_NAME         = name(WebSocketConnection.class, "queueDrainRetry");
  private static final String DISPLACEMENT_COUNTER_NAME              = name(WebSocketConnection.class, "displacement");
  private static final String NON_SUCCESS_RESPONSE_COUNTER_NAME      = name(WebSocketConnection.class, "clientNonSuccessResponse");
  private static final String STATUS_CODE_TAG                        = "status";
  private static final String STATUS_MESSAGE_TAG                     = "message";

  private static final long SLOW_DRAIN_THRESHOLD = 10_000;

  @VisibleForTesting
  static final int MAX_DESKTOP_MESSAGE_SIZE = 1024 * 1024;

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

  private final ScheduledExecutorService retrySchedulingExecutor;

  private final boolean isDesktopClient;

  private final Semaphore processStoredMessagesSemaphore = new Semaphore(1);
  private final AtomicReference<StoredMessageState> storedMessageState = new AtomicReference<>(
      StoredMessageState.PERSISTED_NEW_MESSAGES_AVAILABLE);
  private final AtomicBoolean sentInitialQueueEmptyMessage = new AtomicBoolean(false);
  private final LongAdder sentMessageCounter = new LongAdder();
  private final AtomicLong queueDrainStartTime = new AtomicLong();
  private final AtomicInteger                       consecutiveRetries             = new AtomicInteger();
  private final AtomicReference<ScheduledFuture<?>> retryFuture                    = new AtomicReference<>();

  private final Random random = new Random();

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
      ScheduledExecutorService retrySchedulingExecutor) {

    this(receiptSender,
        messagesManager,
        auth,
        device,
        client,
        DEFAULT_SEND_FUTURES_TIMEOUT_MILLIS,
        retrySchedulingExecutor);
  }

  @VisibleForTesting
  WebSocketConnection(ReceiptSender receiptSender,
      MessagesManager messagesManager,
      AuthenticatedAccount auth,
      Device device,
      WebSocketClient client,
      int sendFuturesTimeoutMillis,
      ScheduledExecutorService retrySchedulingExecutor) {

    this.receiptSender = receiptSender;
    this.messagesManager = messagesManager;
    this.auth = auth;
    this.device = device;
    this.client = client;
    this.sendFuturesTimeoutMillis = sendFuturesTimeoutMillis;
    this.retrySchedulingExecutor = retrySchedulingExecutor;

    Optional<ClientPlatform> maybePlatform;

    try {
      maybePlatform = Optional.of(UserAgentUtil.parseUserAgentString(client.getUserAgent()).getPlatform());
    } catch (final UnrecognizedUserAgentException e) {
      maybePlatform = Optional.empty();
    }

    this.isDesktopClient = maybePlatform.map(platform -> platform == ClientPlatform.DESKTOP).orElse(false);
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

    client.close(1000, "OK");
  }

  private CompletableFuture<WebSocketResponseMessage> sendMessage(final Envelope message, final Optional<StoredMessageInfo> storedMessageInfo) {
    // clear ephemeral field from the envelope
    final Optional<byte[]> body = Optional.ofNullable(message.toBuilder().clearEphemeral().build().toByteArray());

    sendMessageMeter.mark();
    sentMessageCounter.increment();
    bytesSentMeter.mark(body.map(bytes -> bytes.length).orElse(0));

    // X-Signal-Key: false must be sent until Android stops assuming it missing means true
    return client.sendRequest("PUT", "/api/v1/message", List.of("X-Signal-Key: false", TimestampHeaderUtil.getTimestampHeader()), body).whenComplete((response, throwable) -> {
          if (throwable == null) {
            if (isSuccessResponse(response)) {
              if (storedMessageInfo.isPresent()) {
                messagesManager.delete(auth.getAccount().getUuid(), device.getId(), storedMessageInfo.get().getGuid());
              }

              if (message.getType() != Envelope.Type.SERVER_DELIVERY_RECEIPT) {
                recordMessageDeliveryDuration(message.getTimestamp(), device);
                sendDeliveryReceiptFor(message);
              }
            } else {
              final List<Tag> tags = new ArrayList<>(
                  List.of(Tag.of(STATUS_CODE_TAG, String.valueOf(response.getStatus())),
                      UserAgentTagUtil.getPlatformTag(client.getUserAgent())));

          // TODO Remove this once we've identified the cause of message rejections from desktop clients
          if (StringUtils.isNotBlank(response.getMessage())) {
            tags.add(Tag.of(STATUS_MESSAGE_TAG, response.getMessage()));
          }

          Metrics.counter(NON_SUCCESS_RESPONSE_COUNTER_NAME, tags).increment();
        }
      } else {
        sendFailuresMeter.mark();
      }
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
    if (!message.hasSource()) return;

    try {
      receiptSender.sendReceipt(auth, UUID.fromString(message.getSourceUuid()), message.getTimestamp());
    } catch (NoSuchUserException e) {
      logger.info("No longer registered: {}", e.getMessage());
    } catch (WebApplicationException e) {
      logger.warn("Bad federated response for receipt: {}", e.getResponse().getStatus());
    } catch (IllegalArgumentException e) {
      logger.error("Could not parse UUID: {}", message.getSourceUuid());
    }
  }

  private boolean isSuccessResponse(WebSocketResponseMessage response) {
    return response != null && response.getStatus() >= 200 && response.getStatus() < 300;
  }

  @VisibleForTesting
  void processStoredMessages() {
    if (processStoredMessagesSemaphore.tryAcquire()) {
      final StoredMessageState      state              = storedMessageState.getAndSet(StoredMessageState.EMPTY);
      final CompletableFuture<Void> queueClearedFuture = new CompletableFuture<>();

      sendNextMessagePage(state != StoredMessageState.PERSISTED_NEW_MESSAGES_AVAILABLE, queueClearedFuture);

      queueClearedFuture.whenComplete((v, cause) -> {
        if (cause == null) {
          consecutiveRetries.set(0);

          if (sentInitialQueueEmptyMessage.compareAndSet(false, true)) {
            final List<Tag> tags = List.of(UserAgentTagUtil.getPlatformTag(client.getUserAgent()));
            final long drainDuration = System.currentTimeMillis() - queueDrainStartTime.get();

            Metrics.summary(INITIAL_QUEUE_LENGTH_DISTRIBUTION_NAME, tags).record(sentMessageCounter.sum());
            Metrics.timer(INITIAL_QUEUE_DRAIN_TIMER_NAME, tags).record(drainDuration, TimeUnit.MILLISECONDS);

            if (drainDuration > SLOW_DRAIN_THRESHOLD) {
              Metrics.counter(SLOW_QUEUE_DRAIN_COUNTER_NAME, tags).increment();
            }

            client.sendRequest("PUT", "/api/v1/queue/empty",
                Collections.singletonList(TimestampHeaderUtil.getTimestampHeader()), Optional.empty());
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
            logger.debug("Failed to clear queue", cause);

            if (consecutiveRetries.incrementAndGet() > MAX_CONSECUTIVE_RETRIES) {
              client.close(1011, "Failed to retrieve messages");
            } else {
              final List<Tag> tags = List.of(UserAgentTagUtil.getPlatformTag(client.getUserAgent()));

              Metrics.counter(QUEUE_DRAIN_RETRY_COUNTER_NAME, tags).increment();

              final long delay = RETRY_DELAY_MILLIS + random.nextInt(RETRY_DELAY_JITTER_MILLIS);
              retryFuture
                  .set(retrySchedulingExecutor.schedule(this::processStoredMessages, delay, TimeUnit.MILLISECONDS));
            }
          } else {
            logger.debug("Client disconnected before queue cleared");
          }
        }
      });
    }
  }

  private void sendNextMessagePage(final boolean cachedMessagesOnly, final CompletableFuture<Void> queueClearedFuture) {
    try {
      final OutgoingMessageEntityList messages = messagesManager
          .getMessagesForDevice(auth.getAccount().getUuid(), device.getId(), client.getUserAgent(), cachedMessagesOnly);

      final CompletableFuture<?>[] sendFutures = new CompletableFuture[messages.getMessages().size()];

      for (int i = 0; i < messages.getMessages().size(); i++) {
        final OutgoingMessageEntity message = messages.getMessages().get(i);
        final Envelope.Builder builder = Envelope.newBuilder()
            .setType(Envelope.Type.forNumber(message.getType()))
            .setTimestamp(message.getTimestamp())
            .setServerTimestamp(message.getServerTimestamp());

        if (!Util.isEmpty(message.getSource())) {
          builder.setSource(message.getSource())
              .setSourceDevice(message.getSourceDevice());
          if (message.getSourceUuid() != null) {
            builder.setSourceUuid(message.getSourceUuid().toString());
          }
        }

        if (message.getContent() != null) {
          builder.setContent(ByteString.copyFrom(message.getContent()));
        }

        builder.setDestinationUuid(message.getDestinationUuid().toString());

        builder.setServerGuid(message.getGuid().toString());

        final Envelope envelope = builder.build();

        if (envelope.getSerializedSize() > MAX_DESKTOP_MESSAGE_SIZE && isDesktopClient) {
          messagesManager.delete(auth.getAccount().getUuid(), device.getId(), message.getGuid());
          discardedMessagesMeter.mark();

          sendFutures[i] = CompletableFuture.completedFuture(null);
        } else {
          sendFutures[i] = sendMessage(builder.build(), Optional.of(new StoredMessageInfo(message.getGuid())));
        }
      }

      // Set a large, non-zero timeout, to prevent any failure to acknowledge receipt from blocking indefinitely
      CompletableFuture.allOf(sendFutures)
          .orTimeout(sendFuturesTimeoutMillis, TimeUnit.MILLISECONDS)
          .whenComplete((v, cause) -> {
            if (cause == null) {
              if (messages.hasMore()) {
                sendNextMessagePage(cachedMessagesOnly, queueClearedFuture);
              } else {
                queueClearedFuture.complete(null);
              }
            } else {
              queueClearedFuture.completeExceptionally(cause);
            }
          });
    } catch (final Exception e) {
      queueClearedFuture.completeExceptionally(e);
    }
  }

  @Override
  public void handleNewMessagesAvailable() {
    messageAvailableMeter.mark();

    storedMessageState.compareAndSet(StoredMessageState.EMPTY, StoredMessageState.CACHED_NEW_MESSAGES_AVAILABLE);
    processStoredMessages();
  }

  @Override
  public void handleMessagesPersisted() {
    messagesPersistedMeter.mark();

    storedMessageState.set(StoredMessageState.PERSISTED_NEW_MESSAGES_AVAILABLE);
    processStoredMessages();
  }

  @Override
  public void handleDisplacement() {
    Metrics.counter(DISPLACEMENT_COUNTER_NAME, List.of(UserAgentTagUtil.getPlatformTag(client.getUserAgent()))).increment();

    try {
      client.close(1000, "OK");
    } catch (final Exception e) {
      logger.warn("Orderly close failed", e);

      client.hardDisconnectQuietly();
    }
  }

  private static class StoredMessageInfo {
    private final UUID guid;

    public StoredMessageInfo(UUID guid) {
      this.guid = guid;
    }

    public UUID getGuid() {
      return guid;
    }
  }
}
