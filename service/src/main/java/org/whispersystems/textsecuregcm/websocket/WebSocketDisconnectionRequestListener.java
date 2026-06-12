/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.websocket;

import org.whispersystems.textsecuregcm.auth.DisconnectionRequestListener;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import org.whispersystems.websocket.WebSocketClient;

class WebSocketDisconnectionRequestListener implements DisconnectionRequestListener {

  private final MessageMetrics messageMetrics;
  private final String metricChannel;
  private final WebSocketClient client;

  WebSocketDisconnectionRequestListener(final MessageMetrics messageMetrics, final WebSocketClient client, final boolean disableMessages) {
    this.messageMetrics = messageMetrics;
    this.client = client;
    this.metricChannel = disableMessages
        ? MessageMetrics.MESSAGELESS_WEBSOCKET_CHANNEL
        : MessageMetrics.WEBSOCKET_CHANNEL;
  }

  @Override
  public void handleDisconnectionRequest() {
    final UserAgent userAgent = UserAgentUtil.maybeParseUserAgentString(client.getUserAgent());
    messageMetrics.measureMessageStreamDisplaced(metricChannel, userAgent, false);
    client.close(4401, "Reauthentication required");
  }
}
