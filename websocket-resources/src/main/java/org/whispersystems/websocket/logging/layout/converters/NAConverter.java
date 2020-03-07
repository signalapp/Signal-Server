package org.whispersystems.websocket.logging.layout.converters;

import org.whispersystems.websocket.logging.WebsocketEvent;

public class NAConverter extends WebSocketEventConverter {
  @Override
  public String convert(WebsocketEvent event) {
    return WebsocketEvent.NA;
  }
}
