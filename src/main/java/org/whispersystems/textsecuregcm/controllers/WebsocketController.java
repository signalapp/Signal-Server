package org.whispersystems.textsecuregcm.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.websocket.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.AcknowledgeWebsocketMessage;
import org.whispersystems.textsecuregcm.entities.IncomingWebsocketMessage;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.PubSubListener;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.PubSubMessage;
import org.whispersystems.textsecuregcm.storage.StoredMessageManager;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;
import org.whispersystems.textsecuregcm.websocket.WebsocketMessage;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class WebsocketController implements WebSocket.OnTextMessage, PubSubListener {

  private static final Logger            logger          = LoggerFactory.getLogger(WebsocketController.class);
  private static final ObjectMapper      mapper          = new ObjectMapper();
  private static final Map<Long, String> pendingMessages = new HashMap<>();

  private final StoredMessageManager storedMessageManager;
  private final PubSubManager        pubSubManager;

  private final Account account;
  private final Device  device;

  private Connection connection;
  private long       pendingMessageSequence;

  public WebsocketController(StoredMessageManager storedMessageManager,
                             PubSubManager pubSubManager,
                             Account account)
  {
    this.storedMessageManager = storedMessageManager;
    this.pubSubManager        = pubSubManager;
    this.account              = account;
    this.device               = account.getAuthenticatedDevice().get();
  }


  @Override
  public void onOpen(Connection connection) {
    this.connection = connection;
    pubSubManager.subscribe(new WebsocketAddress(this.account.getId(), this.device.getId()), this);
    handleQueryDatabase();
  }

  @Override
  public void onClose(int i, String s) {
    handleClose();
  }

  @Override
  public void onMessage(String body) {
    try {
      IncomingWebsocketMessage incomingMessage = mapper.readValue(body, IncomingWebsocketMessage.class);

      switch (incomingMessage.getType()) {
        case IncomingWebsocketMessage.TYPE_ACKNOWLEDGE_MESSAGE: handleMessageAck(body); break;
        case IncomingWebsocketMessage.TYPE_PING_MESSAGE:        handlePing();           break;
        default:                                                handleClose();          break;
      }
    } catch (IOException e) {
      logger.debug("Parse", e);
      handleClose();
    }
  }

  @Override
  public void onPubSubMessage(PubSubMessage outgoingMessage) {
    switch (outgoingMessage.getType()) {
      case PubSubMessage.TYPE_DELIVER:  handleDeliverOutgoingMessage(outgoingMessage.getContents()); break;
      case PubSubMessage.TYPE_QUERY_DB: handleQueryDatabase();                                       break;
      default:
        logger.warn("Unknown pubsub message: " + outgoingMessage.getType());
    }
  }

  private void handleDeliverOutgoingMessage(String message) {
    try {
      long messageSequence;

      synchronized (pendingMessages) {
        messageSequence = pendingMessageSequence++;
        pendingMessages.put(messageSequence, message);
      }

      connection.sendMessage(mapper.writeValueAsString(new WebsocketMessage(messageSequence, message)));
    } catch (IOException e) {
      logger.debug("Response failed", e);
      handleClose();
    }
  }

  private void handleMessageAck(String message) {
    try {
      AcknowledgeWebsocketMessage ack = mapper.readValue(message, AcknowledgeWebsocketMessage.class);

      synchronized (pendingMessages) {
        pendingMessages.remove(ack.getId());
      }
    } catch (IOException e) {
      logger.warn("Mapping", e);
    }
  }

  private void handlePing() {
    try {
      IncomingWebsocketMessage pongMessage = new IncomingWebsocketMessage(IncomingWebsocketMessage.TYPE_PONG_MESSAGE);
      connection.sendMessage(mapper.writeValueAsString(pongMessage));
    } catch (IOException e) {
      logger.warn("Pong failed", e);
      handleClose();
    }
  }

  private void handleClose() {
    pubSubManager.unsubscribe(new WebsocketAddress(account.getId(), device.getId()), this);
    connection.close();

    List<String> remainingMessages = new LinkedList<>();

    synchronized (pendingMessages) {
      Long[] pendingKeys = pendingMessages.keySet().toArray(new Long[0]);
      Arrays.sort(pendingKeys);

      for (long pendingKey : pendingKeys) {
        remainingMessages.add(pendingMessages.get(pendingKey));
      }

      pendingMessages.clear();
    }

    storedMessageManager.storeMessages(account, device, remainingMessages);
  }

  private void handleQueryDatabase() {
    List<String> messages = storedMessageManager.getOutgoingMessages(account, device);

    for (String message : messages) {
      handleDeliverOutgoingMessage(message);
    }
  }

}
