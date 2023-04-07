/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;
import org.whispersystems.textsecuregcm.storage.PubSubProtos;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import org.whispersystems.websocket.setup.WebSocketConnectListener;
import java.util.List;
import java.util.Optional;

public class ProvisioningConnectListener implements WebSocketConnectListener {

  private final ProvisioningManager provisioningManager;

  public ProvisioningConnectListener(final ProvisioningManager provisioningManager) {
    this.provisioningManager = provisioningManager;
  }

  @Override
  public void onWebSocketConnect(WebSocketSessionContext context) {
    final ProvisioningAddress provisioningAddress = ProvisioningAddress.generate();
    context.addWebsocketClosedListener((context1, statusCode, reason) -> provisioningManager.removeListener(provisioningAddress));

    provisioningManager.addListener(provisioningAddress, message -> {
      assert message.getType() == PubSubProtos.PubSubMessage.Type.DELIVER;

      final Optional<byte[]> body = Optional.of(message.getContent().toByteArray());

      context.getClient().sendRequest("PUT", "/v1/message", List.of(HeaderUtils.getTimestampHeader()), body)
          .whenComplete((ignored, throwable) -> context.getClient().close(1000, "Closed"));
    });

    context.getClient().sendRequest("PUT", "/v1/address", List.of(HeaderUtils.getTimestampHeader()),
        Optional.of(MessageProtos.ProvisioningUuid.newBuilder()
            .setUuid(provisioningAddress.getAddress())
            .build()
            .toByteArray()));
  }
}
