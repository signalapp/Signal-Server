/**
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
