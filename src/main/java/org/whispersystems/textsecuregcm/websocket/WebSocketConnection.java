package org.whispersystems.textsecuregcm.websocket;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.CryptoEncodingException;
import org.whispersystems.textsecuregcm.entities.EncryptedOutgoingMessage;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.push.TransientPushFailureException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PubSubListener;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import static org.whispersystems.textsecuregcm.entities.MessageProtos.OutgoingMessageSignal;
import static org.whispersystems.textsecuregcm.storage.PubSubProtos.PubSubMessage;

public class WebSocketConnection implements PubSubListener {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketConnection.class);

  private final AccountsManager  accountsManager;
  private final PushSender       pushSender;
  private final MessagesManager  messagesManager;
  private final PubSubManager    pubSubManager;

  private final Account          account;
  private final Device           device;
  private final WebsocketAddress address;
  private final WebSocketClient  client;

  public WebSocketConnection(AccountsManager accountsManager,
                             PushSender pushSender,
                             MessagesManager messagesManager,
                             PubSubManager pubSubManager,
                             Account account,
                             Device device,
                             WebSocketClient client)
  {
    this.accountsManager = accountsManager;
    this.pushSender      = pushSender;
    this.messagesManager = messagesManager;
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
  public void onPubSubMessage(PubSubMessage pubSubMessage) {
    try {
      switch (pubSubMessage.getType().getNumber()) {
        case PubSubMessage.Type.QUERY_DB_VALUE:
          processStoredMessages();
          break;
        case PubSubMessage.Type.DELIVER_VALUE:
          sendMessage(OutgoingMessageSignal.parseFrom(pubSubMessage.getContent()), Optional.<Long>absent());
          break;
        case PubSubMessage.Type.CLOSE_VALUE:
          client.close(1000, "OK");
          pubSubManager.unsubscribe(address, this);
          break;
        default:
          logger.warn("Unknown pubsub message: " + pubSubMessage.getType().getNumber());
      }
    } catch (InvalidProtocolBufferException e) {
      logger.warn("Protobuf parse error", e);
    }
  }

  private void sendMessage(final OutgoingMessageSignal message,
                           final Optional<Long> storedMessageId)
  {
    try {
      EncryptedOutgoingMessage                   encryptedMessage = new EncryptedOutgoingMessage(message, device.getSignalingKey());
      Optional<byte[]>                           body             = Optional.fromNullable(encryptedMessage.toByteArray());
      ListenableFuture<WebSocketResponseMessage> response         = client.sendRequest("PUT", "/api/v1/message", body);

      Futures.addCallback(response, new FutureCallback<WebSocketResponseMessage>() {
        @Override
        public void onSuccess(@Nullable WebSocketResponseMessage response) {
          boolean isReceipt = message.getType() == OutgoingMessageSignal.Type.RECEIPT_VALUE;

          if (isSuccessResponse(response)) {
            if (storedMessageId.isPresent()) messagesManager.delete(storedMessageId.get());
            if (!isReceipt)                  sendDeliveryReceiptFor(message);
          } else if (!isSuccessResponse(response) & !storedMessageId.isPresent()) {
            requeueMessage(message);
          }
        }

        @Override
        public void onFailure(@Nonnull Throwable throwable) {
          if (!storedMessageId.isPresent()) requeueMessage(message);
        }

        private boolean isSuccessResponse(WebSocketResponseMessage response) {
          return response != null && response.getStatus() >= 200 && response.getStatus() < 300;
        }
      });
    } catch (CryptoEncodingException e) {
      logger.warn("Bad signaling key", e);
    }
  }

  private void requeueMessage(OutgoingMessageSignal message) {
    try {
      pushSender.sendMessage(account, device, message);
    } catch (NotPushRegisteredException | TransientPushFailureException e) {
      logger.warn("requeueMessage", e);
      messagesManager.insert(account.getNumber(), device.getId(), message);
    }
  }

  private void sendDeliveryReceiptFor(OutgoingMessageSignal message) {
    try {
      Optional<Account> source = accountsManager.get(message.getSource());

      if (!source.isPresent()) {
        logger.warn(String.format("Source account disappeared? (%s)", message.getSource()));
        return;
      }

      OutgoingMessageSignal.Builder receipt =
          OutgoingMessageSignal.newBuilder()
                               .setSource(account.getNumber())
                               .setSourceDevice((int) device.getId())
                               .setTimestamp(message.getTimestamp())
                               .setType(OutgoingMessageSignal.Type.RECEIPT_VALUE);

      for (Device device : source.get().getDevices()) {
        pushSender.sendMessage(source.get(), device, receipt.build());
      }
    } catch (NotPushRegisteredException | TransientPushFailureException e) {
      logger.warn("sendDeliveryReceiptFor", "Delivery receipet", e);
    }
  }

  private void processStoredMessages() {
    List<Pair<Long, OutgoingMessageSignal>> messages = messagesManager.getMessagesForDevice(account.getNumber(),
                                                                                            device.getId());

    for (Pair<Long, OutgoingMessageSignal> message : messages) {
      sendMessage(message.second(), Optional.of(message.first()));
    }
  }
}
