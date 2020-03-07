package org.whispersystems.websocket.logging.layout.converters;

import org.whispersystems.websocket.logging.WebsocketEvent;

public class RemoteHostConverter extends WebSocketEventConverter {
  @Override
  public String convert(WebsocketEvent event) {
    return event.getRemoteHost();
  }
}
