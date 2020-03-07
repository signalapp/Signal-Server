package org.whispersystems.websocket.logging.layout.converters;

import org.whispersystems.websocket.logging.WebsocketEvent;

public class StatusCodeConverter extends WebSocketEventConverter {
  @Override
  public String convert(WebsocketEvent event) {
    if (event.getStatusCode() == WebsocketEvent.SENTINEL) {
      return WebsocketEvent.NA;
    } else {
      return Integer.toString(event.getStatusCode());
    }
  }
}
