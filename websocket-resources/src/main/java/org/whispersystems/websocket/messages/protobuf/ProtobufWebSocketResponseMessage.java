/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.messages.protobuf;

import org.whispersystems.websocket.messages.WebSocketResponseMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ProtobufWebSocketResponseMessage implements WebSocketResponseMessage {

  private final SubProtocol.WebSocketResponseMessage message;

  public ProtobufWebSocketResponseMessage(SubProtocol.WebSocketResponseMessage message) {
    this.message = message;
  }

  @Override
  public long getRequestId() {
    return message.getId();
  }

  @Override
  public int getStatus() {
    return message.getStatus();
  }

  @Override
  public String getMessage() {
    return message.getMessage();
  }

  @Override
  public Optional<byte[]> getBody() {
    if (message.hasBody()) {
      return Optional.of(message.getBody().toByteArray());
    } else {
      return Optional.empty();
    }
  }

  @Override
  public Map<String, String> getHeaders() {
    Map<String, String> results = new HashMap<>();

    for (String header : message.getHeadersList()) {
      String[] tokenized = header.split(":");

      if (tokenized.length == 2 && tokenized[0] != null && tokenized[1] != null) {
        results.put(tokenized[0].trim().toLowerCase(), tokenized[1].trim());
      }
    }

    return results;
  }
}
