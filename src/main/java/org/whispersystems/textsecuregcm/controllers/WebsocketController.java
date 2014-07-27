package org.whispersystems.textsecuregcm.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.eclipse.jetty.websocket.api.CloseStatus;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.entities.AcknowledgeWebsocketMessage;
import org.whispersystems.textsecuregcm.entities.IncomingWebsocketMessage;
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
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;
import org.whispersystems.textsecuregcm.websocket.WebsocketMessage;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.basic.BasicCredentials;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.OutgoingMessageSignal;

public class WebsocketController implements WebSocketListener, PubSubListener {

  private static final Logger                    logger          = LoggerFactory.getLogger(WebsocketController.class);
  private static final ObjectMapper              mapper          = SystemMapper.getMapper();
  private static final Map<Long, PendingMessage> pendingMessages = new HashMap<>();

  private final AccountAuthenticator accountAuthenticator;
  private final AccountsManager      accountsManager;
  private final PubSubManager        pubSubManager;
  private final StoredMessages       storedMessages;
  private final PushSender           pushSender;

  private WebsocketAddress address;
  private Account          account;
  private Device           device;
  private Session          session;

  private long pendingMessageSequence;

  public WebsocketController(AccountAuthenticator accountAuthenticator,
                             AccountsManager      accountsManager,
                             PushSender           pushSender,
                             PubSubManager        pubSubManager,
                             StoredMessages       storedMessages)
  {
    this.accountAuthenticator = accountAuthenticator;
    this.accountsManager      = accountsManager;
    this.pushSender           = pushSender;
    this.pubSubManager        = pubSubManager;
    this.storedMessages       = storedMessages;
  }

  @Override
  public void onWebSocketConnect(Session session) {
    try {
      UpgradeRequest        request    = session.getUpgradeRequest();
      Map<String, String[]> parameters = request.getParameterMap();
      String[]              usernames  = parameters.get("login"   );
      String[]              passwords  = parameters.get("password");

      if (usernames == null || usernames.length == 0 ||
          passwords == null || passwords.length == 0)
      {
        session.close(new CloseStatus(4001, "Unauthorized"));
        return;
      }

      BasicCredentials  credentials = new BasicCredentials(usernames[0], passwords[0]);
      Optional<Account> account     = accountAuthenticator.authenticate(credentials);

      if (!account.isPresent()) {
        session.close(new CloseStatus(4001, "Unauthorized"));
        return;
      }

      this.account = account.get();
      this.device  = account.get().getAuthenticatedDevice().get();
      this.address = new WebsocketAddress(this.account.getNumber(), this.device.getId());
      this.session = session;

      this.session.setIdleTimeout(10 * 60 * 1000);
      this.pubSubManager.subscribe(this.address, this);

      handleQueryDatabase();
    } catch (AuthenticationException e) {
      try { session.close(1011, "Server Error");} catch (IOException e1) {}
    } catch (IOException ioe) {
      logger.info("Abrupt session close.");
    }
  }

  @Override
  public void onWebSocketText(String body) {
    try {
      IncomingWebsocketMessage incomingMessage = mapper.readValue(body, IncomingWebsocketMessage.class);

      switch (incomingMessage.getType()) {
        case IncomingWebsocketMessage.TYPE_ACKNOWLEDGE_MESSAGE:
          handleMessageAck(body);
          break;
        default:
          close(new CloseStatus(1008, "Unknown Type"));
      }
    } catch (IOException e) {
      logger.debug("Parse", e);
      close(new CloseStatus(1008, "Badly Formatted"));
    }
  }

  @Override
  public void onWebSocketClose(int i, String s) {
    pubSubManager.unsubscribe(this.address, this);

    List<PendingMessage> remainingMessages = new LinkedList<>();

    synchronized (pendingMessages) {
      Long[] pendingKeys = pendingMessages.keySet().toArray(new Long[0]);
      Arrays.sort(pendingKeys);

      for (long pendingKey : pendingKeys) {
        remainingMessages.add(pendingMessages.get(pendingKey));
      }

      pendingMessages.clear();
    }

    for (PendingMessage remainingMessage : remainingMessages) {
      try {
        pushSender.sendMessage(account, device, remainingMessage);
      } catch (NotPushRegisteredException | TransientPushFailureException e) {
        logger.warn("onWebSocketClose", e);
        storedMessages.insert(address, remainingMessage);
      }
    }
  }

  @Override
  public void onPubSubMessage(PubSubMessage outgoingMessage) {
    switch (outgoingMessage.getType()) {
      case PubSubMessage.TYPE_DELIVER:
        try {
          PendingMessage pendingMessage = mapper.readValue(outgoingMessage.getContents(), PendingMessage.class);
          handleDeliverOutgoingMessage(pendingMessage);
        } catch (IOException e) {
          logger.warn("WebsocketController", "Error deserializing PendingMessage", e);
        }
        break;
      case PubSubMessage.TYPE_QUERY_DB:
        handleQueryDatabase();
        break;
      default:
        logger.warn("Unknown pubsub message: " + outgoingMessage.getType());
    }
  }

  private void handleDeliverOutgoingMessage(PendingMessage message) {
    try {
      long messageSequence;

      synchronized (pendingMessages) {
        messageSequence = pendingMessageSequence++;
        pendingMessages.put(messageSequence, message);
      }

      WebsocketMessage websocketMessage = new WebsocketMessage(messageSequence, message.getEncryptedOutgoingMessage());
      session.getRemote().sendStringByFuture(mapper.writeValueAsString(websocketMessage));
    } catch (IOException e) {
      logger.debug("Response failed", e);
      close(null);
    }
  }

  private void handleMessageAck(String message) {
    try {
      AcknowledgeWebsocketMessage ack = mapper.readValue(message, AcknowledgeWebsocketMessage.class);
      PendingMessage acknowledgedMessage;

      synchronized (pendingMessages) {
        acknowledgedMessage = pendingMessages.remove(ack.getId());
      }

      if (acknowledgedMessage != null && !acknowledgedMessage.isReceipt()) {
        sendDeliveryReceipt(acknowledgedMessage);
      }

    } catch (IOException e) {
      logger.warn("Mapping", e);
    }
  }

  private void handleQueryDatabase() {
    List<PendingMessage> messages = storedMessages.getMessagesForDevice(address);

    for (PendingMessage message : messages) {
      handleDeliverOutgoingMessage(message);
    }
  }

  private void sendDeliveryReceipt(PendingMessage acknowledgedMessage) {
    try {
      Optional<Account> source = accountsManager.get(acknowledgedMessage.getSender());

      if (!source.isPresent()) {
        logger.warn("Source account disappeared? (%s)", acknowledgedMessage.getSender());
        return;
      }

      OutgoingMessageSignal.Builder receipt =
          OutgoingMessageSignal.newBuilder()
                               .setSource(account.getNumber())
                               .setSourceDevice((int) device.getId())
                               .setTimestamp(acknowledgedMessage.getMessageId())
                               .setType(OutgoingMessageSignal.Type.RECEIPT_VALUE);

      for (Device device : source.get().getDevices()) {
        pushSender.sendMessage(source.get(), device, receipt.build());
      }
    } catch (NotPushRegisteredException | TransientPushFailureException e) {
      logger.warn("Websocket", "Delivery receipet", e);
    }
  }

  @Override
  public void onWebSocketBinary(byte[] bytes, int i, int i2) {
    logger.info("Received binary message!");
  }

  @Override
  public void onWebSocketError(Throwable throwable) {
    logger.info("onWebSocketError", throwable);
  }


  private void close(CloseStatus closeStatus) {
    try {
      if (this.session != null) {
        if (closeStatus != null) this.session.close(closeStatus);
        else                     this.session.close();
      }
    } catch (IOException e) {
      logger.info("close()", e);
    }
  }
}
