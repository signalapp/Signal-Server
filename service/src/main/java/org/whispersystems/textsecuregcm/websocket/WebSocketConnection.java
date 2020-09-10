package org.whispersystems.textsecuregcm.websocket;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.dispatch.DispatchChannel;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.controllers.NoSuchUserException;
import org.whispersystems.textsecuregcm.entities.CryptoEncodingException;
import org.whispersystems.textsecuregcm.entities.EncryptedOutgoingMessage;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.push.DisplacedPresenceListener;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessageAvailabilityListener;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.TimestampHeaderUtil;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;

import javax.ws.rs.WebApplicationException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.codahale.metrics.MetricRegistry.name;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import static org.whispersystems.textsecuregcm.storage.PubSubProtos.PubSubMessage;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class WebSocketConnection implements DispatchChannel, MessageAvailabilityListener, DisplacedPresenceListener {

  private static final MetricRegistry metricRegistry                 = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  public  static final Histogram      messageTime                    = metricRegistry.histogram(name(MessageController.class, "message_delivery_duration"));
  private static final Meter          sendMessageMeter               = metricRegistry.meter(name(WebSocketConnection.class, "send_message"));
  private static final Meter          messageAvailableMeter          = metricRegistry.meter(name(WebSocketConnection.class, "messagesAvailable"));
  private static final Meter          ephemeralMessageAvailableMeter = metricRegistry.meter(name(WebSocketConnection.class, "ephemeralMessagesAvailable"));
  private static final Meter          messagesPersistedMeter         = metricRegistry.meter(name(WebSocketConnection.class, "messagesPersisted"));
  private static final Meter          pubSubNewMessageMeter          = metricRegistry.meter(name(WebSocketConnection.class, "pubSubNewMessage"));
  private static final Meter          pubSubPersistedMeter           = metricRegistry.meter(name(WebSocketConnection.class, "pubSubPersisted"));
  private static final Meter          displacementMeter              = metricRegistry.meter(name(WebSocketConnection.class, "explicitDisplacement"));

  private static final Logger logger = LoggerFactory.getLogger(WebSocketConnection.class);

  private final ReceiptSender    receiptSender;
  private final PushSender       pushSender;
  private final MessagesManager  messagesManager;

  private final Account          account;
  private final Device           device;
  private final WebSocketClient  client;
  private final String           connectionId;

  private final Semaphore     processStoredMessagesSemaphore = new Semaphore(1);
  private final AtomicBoolean newMessagesAvailable           = new AtomicBoolean(true);
  private final AtomicBoolean persistedMessagesAvailable     = new AtomicBoolean(true);
  private final AtomicBoolean sentInitialQueueEmptyMessage   = new AtomicBoolean(false);

  public WebSocketConnection(PushSender pushSender,
                             ReceiptSender receiptSender,
                             MessagesManager messagesManager,
                             Account account,
                             Device device,
                             WebSocketClient client,
                             String connectionId)
  {
    this.pushSender      = pushSender;
    this.receiptSender   = receiptSender;
    this.messagesManager = messagesManager;
    this.account         = account;
    this.device          = device;
    this.client          = client;
    this.connectionId    = connectionId;
  }

  @Override
  public void onDispatchMessage(String channel, byte[] message) {
    try {
      PubSubMessage pubSubMessage = PubSubMessage.parseFrom(message);

      switch (pubSubMessage.getType().getNumber()) {
        case PubSubMessage.Type.QUERY_DB_VALUE:
          pubSubPersistedMeter.mark();
          newMessagesAvailable.set(true);
          processStoredMessages();
          break;
        case PubSubMessage.Type.DELIVER_VALUE:
          pubSubNewMessageMeter.mark();
          sendMessage(Envelope.parseFrom(pubSubMessage.getContent()), Optional.empty());
          break;
        default:
          logger.warn("Unknown pubsub message: " + pubSubMessage.getType().getNumber());
      }
    } catch (InvalidProtocolBufferException e) {
      logger.warn("Protobuf parse error", e);
    }
  }

  @Override
  public void onDispatchUnsubscribed(String channel) {
    client.close(1000, "OK");
  }

  public void onDispatchSubscribed(String channel) {
    processStoredMessages();
  }

  private CompletableFuture<Void> sendMessage(final Envelope message, final Optional<StoredMessageInfo> storedMessageInfo) {
    try {
      String           header;
      Optional<byte[]> body;

      if (Util.isEmpty(device.getSignalingKey())) {
        header = "X-Signal-Key: false";
        body   = Optional.ofNullable(message.toByteArray());
      } else {
        header = "X-Signal-Key: true";
        body   = Optional.ofNullable(new EncryptedOutgoingMessage(message, device.getSignalingKey()).toByteArray());
      }

      sendMessageMeter.mark();

      return client.sendRequest("PUT", "/api/v1/message", List.of(header, TimestampHeaderUtil.getTimestampHeader()), body)
            .thenAccept(response -> {
              boolean isReceipt = message.getType() == Envelope.Type.RECEIPT;

              if (isSuccessResponse(response) && !isReceipt) {
                messageTime.update(System.currentTimeMillis() - message.getTimestamp());
              }

              if (isSuccessResponse(response)) {
                if (storedMessageInfo.isPresent()) messagesManager.delete(account.getNumber(), account.getUuid(), device.getId(), storedMessageInfo.get().id, storedMessageInfo.get().cached);
                if (!isReceipt)                    sendDeliveryReceiptFor(message);
              } else if (!isSuccessResponse(response) && !storedMessageInfo.isPresent()) {
                requeueMessage(message);
              }
            })
            .exceptionally(throwable ->  {
              if (!storedMessageInfo.isPresent()) requeueMessage(message);
              return null;
            });
    } catch (CryptoEncodingException e) {
      logger.warn("Bad signaling key", e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private void requeueMessage(Envelope message) {
    pushSender.getWebSocketSender().queueMessage(account, device, message);

    try {
      pushSender.sendQueuedNotification(account, device);
    } catch (NotPushRegisteredException e) {
      logger.warn("requeueMessage", e);
    }
  }

  private void sendDeliveryReceiptFor(Envelope message) {
    if (!message.hasSource()) return;

    try {
      receiptSender.sendReceipt(account, message.getSource(), message.getTimestamp());
    } catch (NoSuchUserException e) {
      logger.info("No longer registered " + e.getMessage());
    } catch (WebApplicationException e) {
      logger.warn("Bad federated response for receipt: " + e.getResponse().getStatus());
    }
  }

  private boolean isSuccessResponse(WebSocketResponseMessage response) {
    return response != null && response.getStatus() >= 200 && response.getStatus() < 300;
  }

  @VisibleForTesting
  void processStoredMessages() {
    if (processStoredMessagesSemaphore.tryAcquire()) {
      if (newMessagesAvailable.getAndSet(false)) {
        sendNextMessagePage(!persistedMessagesAvailable.getAndSet(false));
      } else {
        processStoredMessagesSemaphore.release();
      }
    }
  }

  private void sendNextMessagePage(final boolean cachedMessagesOnly) {
    final OutgoingMessageEntityList messages    = messagesManager.getMessagesForDevice(account.getNumber(), account.getUuid(), device.getId(), client.getUserAgent(), cachedMessagesOnly);
    final CompletableFuture<?>[]    sendFutures = new CompletableFuture[messages.getMessages().size()];

    for (int i = 0; i < messages.getMessages().size(); i++) {
      final OutgoingMessageEntity message = messages.getMessages().get(i);
      final Envelope.Builder      builder = Envelope.newBuilder()
                                                    .setType(Envelope.Type.valueOf(message.getType()))
                                                    .setTimestamp(message.getTimestamp())
                                                    .setServerTimestamp(message.getServerTimestamp());

      if (!Util.isEmpty(message.getSource())) {
        builder.setSource(message.getSource())
                .setSourceDevice(message.getSourceDevice());
      }

      if (message.getMessage() != null) {
        builder.setLegacyMessage(ByteString.copyFrom(message.getMessage()));
      }

      if (message.getContent() != null) {
        builder.setContent(ByteString.copyFrom(message.getContent()));
      }

      if (message.getRelay() != null && !message.getRelay().isEmpty()) {
        builder.setRelay(message.getRelay());
      }

      sendFutures[i] = sendMessage(builder.build(), Optional.of(new StoredMessageInfo(message.getId(), message.isCached())));
    }

    CompletableFuture.allOf(sendFutures).whenComplete((v, cause) -> {
      if (messages.hasMore()) {
        sendNextMessagePage(cachedMessagesOnly);
      } else {
        if (sentInitialQueueEmptyMessage.compareAndSet(false, true)) {
          client.sendRequest("PUT", "/api/v1/queue/empty", Collections.singletonList(TimestampHeaderUtil.getTimestampHeader()), Optional.empty());
        }

        processStoredMessagesSemaphore.release();
        processStoredMessages();
      }
    });
  }

  @Override
  public void handleNewMessagesAvailable() {
    messageAvailableMeter.mark();

    newMessagesAvailable.set(true);
    processStoredMessages();
  }

  @Override
  public void handleNewEphemeralMessageAvailable() {
    ephemeralMessageAvailableMeter.mark();

    messagesManager.takeEphemeralMessage(account.getUuid(), device.getId())
                   .ifPresent(message -> sendMessage(message, Optional.empty()));
  }

  @Override
  public void handleMessagesPersisted() {
    messagesPersistedMeter.mark();

    persistedMessagesAvailable.set(true);
    newMessagesAvailable.set(true);
    processStoredMessages();
  }

  @Override
  public void handleDisplacement() {
    displacementMeter.mark();
    client.hardDisconnectQuietly();
  }

  private static class StoredMessageInfo {
    private final long    id;
    private final boolean cached;

    private StoredMessageInfo(long id, boolean cached) {
      this.id     = id;
      this.cached = cached;
    }
  }
}
