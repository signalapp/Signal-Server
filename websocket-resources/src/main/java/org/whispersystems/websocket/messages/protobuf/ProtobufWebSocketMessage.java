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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.whispersystems.websocket.messages.InvalidMessageException;
import org.whispersystems.websocket.messages.WebSocketMessage;
import org.whispersystems.websocket.messages.WebSocketRequestMessage;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;

public class ProtobufWebSocketMessage implements WebSocketMessage {

  private final SubProtocol.WebSocketMessage message;

  ProtobufWebSocketMessage(byte[] buffer, int offset, int length) throws InvalidMessageException {
    try {
      this.message = SubProtocol.WebSocketMessage.parseFrom(ByteString.copyFrom(buffer, offset, length));

      if (getType() == Type.REQUEST_MESSAGE) {
        if (!message.getRequest().hasVerb() || !message.getRequest().hasPath()) {
          throw new InvalidMessageException("Missing required request attributes!");
        }
      } else if (getType() == Type.RESPONSE_MESSAGE) {
        if (!message.getResponse().hasId() || !message.getResponse().hasStatus() || !message.getResponse().hasMessage()) {
          throw new InvalidMessageException("Missing required response attributes!");
        }
      }
    } catch (InvalidProtocolBufferException e) {
      throw new InvalidMessageException(e);
    }
  }

  ProtobufWebSocketMessage(SubProtocol.WebSocketMessage message) {
    this.message = message;
  }

  @Override
  public Type getType() {
    if (message.getType().getNumber() == SubProtocol.WebSocketMessage.Type.REQUEST_VALUE &&
        message.hasRequest())
    {
      return Type.REQUEST_MESSAGE;
    } else if (message.getType().getNumber() == SubProtocol.WebSocketMessage.Type.RESPONSE_VALUE &&
               message.hasResponse())
    {
      return Type.RESPONSE_MESSAGE;
    } else {
      return Type.UNKNOWN_MESSAGE;
    }
  }

  @Override
  public WebSocketRequestMessage getRequestMessage() {
    return new ProtobufWebSocketRequestMessage(message.getRequest());
  }

  @Override
  public WebSocketResponseMessage getResponseMessage() {
    return new ProtobufWebSocketResponseMessage(message.getResponse());
  }

  @Override
  public byte[] toByteArray() {
    return message.toByteArray();
  }
}
