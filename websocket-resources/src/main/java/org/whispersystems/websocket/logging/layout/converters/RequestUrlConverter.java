package org.whispersystems.websocket.logging.layout.converters;

import org.whispersystems.websocket.logging.WebsocketEvent;

public class RequestUrlConverter extends WebSocketEventConverter {
  @Override
  public String convert(WebsocketEvent event) {
    return
        event.getMethod()                  +
        WebSocketEventConverter.SPACE_CHAR +
        event.getPath()                    +
        WebSocketEventConverter.SPACE_CHAR +
        event.getProtocol();
  }
}
