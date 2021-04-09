/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.dispatch.DispatchChannel;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PubSubProtos.PubSubMessage;

import java.util.Optional;

import static com.codahale.metrics.MetricRegistry.name;

public class DeadLetterHandler implements DispatchChannel {

  private final Logger logger = LoggerFactory.getLogger(DeadLetterHandler.class);

  private final AccountsManager accountsManager;
  private final MessagesManager messagesManager;

  private final Counter deadLetterCounter = Metrics.counter(name(getClass(), "deadLetterCounter"));

  public DeadLetterHandler(AccountsManager accountsManager, MessagesManager messagesManager) {
    this.accountsManager = accountsManager;
    this.messagesManager = messagesManager;
  }

  @Override
  public void onDispatchMessage(String channel, byte[] data) {
    try {
      logger.info("Handling dead letter to: " + channel);
      deadLetterCounter.increment();

      WebsocketAddress address       = new WebsocketAddress(channel);
      PubSubMessage    pubSubMessage = PubSubMessage.parseFrom(data);

      switch (pubSubMessage.getType().getNumber()) {
        case PubSubMessage.Type.DELIVER_VALUE:
          Envelope          message      = Envelope.parseFrom(pubSubMessage.getContent());
          Optional<Account> maybeAccount = accountsManager.get(address.getNumber());

          if (maybeAccount.isPresent()) {
            messagesManager.insert(maybeAccount.get().getUuid(), address.getDeviceId(), message);
          } else {
            logger.warn("Dead letter for account that no longer exists: {}", address);
          }

          break;
      }
    } catch (InvalidProtocolBufferException e) {
      logger.warn("Bad pubsub message", e);
    } catch (InvalidWebsocketAddressException e) {
      logger.warn("Invalid websocket address", e);
    }
  }

  @Override
  public void onDispatchSubscribed(String channel) {
    logger.warn("DeadLetterHandler subscription notice! " + channel);
  }

  @Override
  public void onDispatchUnsubscribed(String channel) {
    logger.warn("DeadLetterHandler unsubscribe notice! " + channel);
  }
}
