/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.setup;

import org.whispersystems.websocket.session.WebSocketSessionContext;

public interface WebSocketConnectListener {
  public void onWebSocketConnect(WebSocketSessionContext context);
}
