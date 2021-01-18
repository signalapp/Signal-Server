package org.whispersystems.textsecuregcm.websocket;

import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import org.whispersystems.websocket.setup.WebSocketConnectListener;

public class ProvisioningConnectListener implements WebSocketConnectListener {

  private final PubSubManager pubSubManager;

  public ProvisioningConnectListener(PubSubManager pubSubManager) {
    this.pubSubManager = pubSubManager;
  }

  @Override
  public void onWebSocketConnect(WebSocketSessionContext context) {
    final ProvisioningConnection connection          = new ProvisioningConnection(context.getClient());
    final ProvisioningAddress    provisioningAddress = ProvisioningAddress.generate();

    pubSubManager.subscribe(provisioningAddress, connection);

    context.addListener((context1, statusCode, reason) -> pubSubManager.unsubscribe(provisioningAddress, connection));
  }
}
