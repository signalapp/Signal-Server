/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.logging.layout.converters;

import org.whispersystems.websocket.logging.WebsocketEvent;

public class NAConverter extends WebSocketEventConverter {
  @Override
  public String convert(WebsocketEvent event) {
    return WebsocketEvent.NA;
  }
}
