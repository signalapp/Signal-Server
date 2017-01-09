package org.whispersystems.textsecuregcm.websocket;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.dispatch.DispatchChannel;
import org.whispersystems.textsecuregcm.entities.MessageProtos.ProvisioningUuid;
import org.whispersystems.textsecuregcm.storage.PubSubProtos.PubSubMessage;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;

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

        ListenableFuture<WebSocketResponseMessage> response = client.sendRequest("PUT", "/v1/message", null, body);

        Futures.addCallback(response, new FutureCallback<WebSocketResponseMessage>() {
          @Override
          public void onSuccess(WebSocketResponseMessage webSocketResponseMessage) {
            client.close(1001, "All you get.");
          }

          @Override
          public void onFailure(Throwable throwable) {
            client.close(1001, "That's all!");
          }
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
      this.client.sendRequest("PUT", "/v1/address", null, Optional.of(ProvisioningUuid.newBuilder()
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
