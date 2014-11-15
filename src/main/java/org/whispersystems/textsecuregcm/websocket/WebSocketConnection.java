package org.whispersystems.textsecuregcm.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.PendingMessage;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.push.TransientPushFailureException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.PubSubListener;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.PubSubMessage;
import org.whispersystems.textsecuregcm.storage.StoredMessages;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static org.whispersystems.textsecuregcm.entities.MessageProtos.OutgoingMessageSignal;

public class WebSocketConnection implements PubSubListener {

  private static final Logger       logger       = LoggerFactory.getLogger(WebSocketConnection.class);
  private static final ObjectMapper objectMapper = SystemMapper.getMapper();

  private final AccountsManager  accountsManager;
  private final PushSender       pushSender;
  private final StoredMessages   storedMessages;
  private final PubSubManager    pubSubManager;

  private final Account          account;
  private final Device           device;
  private final WebsocketAddress address;
  private final WebSocketClient  client;

  public WebSocketConnection(AccountsManager accountsManager,
                             PushSender pushSender,
                             StoredMessages storedMessages,
                             PubSubManager pubSubManager,
                             Account account,
                             Device device,
                             WebSocketClient client)
  {
    this.accountsManager = accountsManager;
    this.pushSender      = pushSender;
    this.storedMessages  = storedMessages;
    this.pubSubManager   = pubSubManager;
    this.account         = account;
    this.device          = device;
    this.client          = client;
    this.address         = new WebsocketAddress(account.getNumber(), device.getId());
  }

  public void onConnected() {
    pubSubManager.subscribe(address, this);
    processStoredMessages();
  }

  public void onConnectionLost() {
    pubSubManager.unsubscribe(address, this);
  }

  @Override
  public void onPubSubMessage(PubSubMessage message) {
    try {
      switch (message.getType()) {
        case PubSubMessage.TYPE_QUERY_DB:
          processStoredMessages();
          break;
        case PubSubMessage.TYPE_DELIVER:
          PendingMessage pendingMessage = objectMapper.readValue(message.getContents(),
                                                                 PendingMessage.class);
          sendMessage(pendingMessage);
          break;
        default:
          logger.warn("Unknown pubsub message: " + message.getType());
      }
    } catch (IOException e) {
      logger.warn("Error deserializing PendingMessage", e);
    }
  }

  private void sendMessage(final PendingMessage message) {
    String                                     content  = message.getEncryptedOutgoingMessage();
    Optional<byte[]>                           body     = Optional.fromNullable(content.getBytes());
    ListenableFuture<WebSocketResponseMessage> response = client.sendRequest("PUT", "/api/v1/message", body);

    Futures.addCallback(response, new FutureCallback<WebSocketResponseMessage>() {
      @Override
      public void onSuccess(@Nullable WebSocketResponseMessage response) {
        if (isSuccessResponse(response) && !message.isReceipt()) {
          sendDeliveryReceiptFor(message);
        } else if (!isSuccessResponse(response)) {
          requeueMessage(message);
        }
      }

      @Override
      public void onFailure(@Nonnull Throwable throwable) {
        requeueMessage(message);
      }

      private boolean isSuccessResponse(WebSocketResponseMessage response) {
        return response != null && response.getStatus() >= 200 && response.getStatus() < 300;
      }
    });
  }

  private void requeueMessage(PendingMessage message) {
    try {
      pushSender.sendMessage(account, device, message);
    } catch (NotPushRegisteredException | TransientPushFailureException e) {
      logger.warn("requeueMessage", e);
      storedMessages.insert(address, message);
    }
  }

  private void sendDeliveryReceiptFor(PendingMessage message) {
    try {
      Optional<Account> source = accountsManager.get(message.getSender());

      if (!source.isPresent()) {
        logger.warn("Source account disappeared? (%s)", message.getSender());
        return;
      }

      OutgoingMessageSignal.Builder receipt =
          OutgoingMessageSignal.newBuilder()
                               .setSource(account.getNumber())
                               .setSourceDevice((int) device.getId())
                               .setTimestamp(message.getMessageId())
                               .setType(OutgoingMessageSignal.Type.RECEIPT_VALUE);

      for (Device device : source.get().getDevices()) {
        pushSender.sendMessage(source.get(), device, receipt.build());
      }
    } catch (NotPushRegisteredException | TransientPushFailureException e) {
      logger.warn("sendDeliveryReceiptFor", "Delivery receipet", e);
    }
  }

  private void processStoredMessages() {
    List<PendingMessage> messages = storedMessages.getMessagesForDevice(address);

    for (PendingMessage message : messages) {
      sendMessage(message);
    }
  }
}
