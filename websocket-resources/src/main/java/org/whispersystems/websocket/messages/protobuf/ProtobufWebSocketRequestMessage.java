/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.messages.protobuf;

import org.whispersystems.websocket.messages.WebSocketRequestMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ProtobufWebSocketRequestMessage implements WebSocketRequestMessage {

  private final SubProtocol.WebSocketRequestMessage message;

  ProtobufWebSocketRequestMessage(SubProtocol.WebSocketRequestMessage message) {
    this.message = message;
  }

  @Override
  public String getVerb() {
    return message.getVerb();
  }

  @Override
  public String getPath() {
    return message.getPath();
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
  public long getRequestId() {
    return message.getId();
  }

  @Override
  public boolean hasRequestId() {
    return message.hasId();
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
