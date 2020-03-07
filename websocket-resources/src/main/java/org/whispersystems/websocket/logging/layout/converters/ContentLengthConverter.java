package org.whispersystems.websocket.logging.layout.converters;

import org.whispersystems.websocket.logging.WebsocketEvent;

public class ContentLengthConverter extends WebSocketEventConverter {
  @Override
  public String convert(WebsocketEvent event) {
    if (event.getContentLength() == WebsocketEvent.SENTINEL) {
      return WebsocketEvent.NA;
    } else {
      return Long.toString(event.getContentLength());
    }
  }
}
