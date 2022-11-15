/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Collections;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.dispatch.DispatchChannel;
import org.whispersystems.textsecuregcm.entities.MessageProtos.ProvisioningUuid;
import org.whispersystems.textsecuregcm.storage.PubSubProtos.PubSubMessage;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.websocket.WebSocketClient;

public class ProvisioningConnection implements DispatchChannel {

  private final Logger logger = LoggerFactory.getLogger(ProvisioningConnection.class);

  private final WebSocketClient client;

  public ProvisioningConnection(WebSocketClient client) {
    this.client = client;
  }

  @Override
  public void onDispatchMessage(String channel, byte[] message) {
    try {
      PubSubMessage outgoingMessage = PubSubMessage.parseFrom(message);

      if (outgoingMessage.getType() == PubSubMessage.Type.DELIVER) {
        Optional<byte[]> body = Optional.of(outgoingMessage.getContent().toByteArray());

        client.sendRequest("PUT", "/v1/message", Collections.singletonList(HeaderUtils.getTimestampHeader()), body)
              .thenAccept(response -> client.close(1001, "All you get."))
              .exceptionally(throwable -> {
                client.close(1001, "That's all!");
                return null;
              });
      }
    } catch (InvalidProtocolBufferException e) {
      logger.warn("Protobuf Error: ", e);
    }
  }

  @Override
  public void onDispatchSubscribed(String channel) {
    try {
      ProvisioningAddress address = new ProvisioningAddress(channel);
      this.client.sendRequest("PUT", "/v1/address", Collections.singletonList(HeaderUtils.getTimestampHeader()),
              Optional.of(ProvisioningUuid.newBuilder()
                      .setUuid(address.getAddress())
                      .build()
                      .toByteArray()));

    } catch (InvalidWebsocketAddressException e) {
      logger.warn("Badly formatted address", e);
      this.client.close(1001, "Server Error");
    }
  }

  @Override
  public void onDispatchUnsubscribed(String channel) {
    this.client.close(1001, "Closed");
  }
}
