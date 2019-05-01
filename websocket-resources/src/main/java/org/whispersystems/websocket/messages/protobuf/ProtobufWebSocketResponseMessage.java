/*
 * Copyright (C) 2014 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
