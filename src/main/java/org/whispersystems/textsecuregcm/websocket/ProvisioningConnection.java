package org.whispersystems.textsecuregcm.websocket;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.whispersystems.textsecuregcm.entities.MessageProtos.ProvisioningUuid;
import org.whispersystems.textsecuregcm.storage.PubSubListener;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.PubSubProtos.PubSubMessage;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;

public class ProvisioningConnection implements PubSubListener {

  private final PubSubManager       pubSubManager;
  private final ProvisioningAddress provisioningAddress;
  private final WebSocketClient     client;

  public ProvisioningConnection(PubSubManager pubSubManager, WebSocketClient client) {
    this.pubSubManager       = pubSubManager;
    this.client              = client;
    this.provisioningAddress = ProvisioningAddress.generate();
  }

  @Override
  public void onPubSubMessage(PubSubMessage outgoingMessage) {
    if (outgoingMessage.getType() == PubSubMessage.Type.DELIVER) {
      Optional<byte[]> body = Optional.of(outgoingMessage.getContent().toByteArray());

      ListenableFuture<WebSocketResponseMessage> response = client.sendRequest("PUT", "/v1/message", body);

      Futures.addCallback(response, new FutureCallback<WebSocketResponseMessage>() {
        @Override
        public void onSuccess(WebSocketResponseMessage webSocketResponseMessage) {
          pubSubManager.unsubscribe(provisioningAddress, ProvisioningConnection.this);
          client.close(1001, "All you get.");
        }

        @Override
        public void onFailure(Throwable throwable) {
          pubSubManager.unsubscribe(provisioningAddress, ProvisioningConnection.this);
          client.close(1001, "That's all!");
        }
      });
    }
  }

  public void onConnected() {
    this.pubSubManager.subscribe(provisioningAddress, this);
    this.client.sendRequest("PUT", "/v1/address", Optional.of(ProvisioningUuid.newBuilder()
                                                                              .setUuid(provisioningAddress.getAddress())
                                                                              .build()
                                                                              .toByteArray()));
  }

  public void onConnectionLost() {
    this.pubSubManager.unsubscribe(provisioningAddress, this);
    this.client.close(1001, "Done");
  }
}
