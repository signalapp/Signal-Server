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
import org.whispersystems.websocket.messages.InvalidMessageException;
import org.whispersystems.websocket.messages.WebSocketMessage;
import org.whispersystems.websocket.messages.WebSocketMessageFactory;

import java.util.List;
import java.util.Optional;

public class ProtobufWebSocketMessageFactory implements WebSocketMessageFactory {

  @Override
  public WebSocketMessage parseMessage(byte[] serialized, int offset, int len)
      throws InvalidMessageException
  {
    return new ProtobufWebSocketMessage(serialized, offset, len);
  }

  @Override
  public WebSocketMessage createRequest(Optional<Long> requestId,
                                        String verb, String path,
                                        List<String> headers,
                                        Optional<byte[]> body)
  {
    SubProtocol.WebSocketRequestMessage.Builder requestMessage =
        SubProtocol.WebSocketRequestMessage.newBuilder()
                                           .setVerb(verb)
                                           .setPath(path);

    if (requestId.isPresent()) {
      requestMessage.setId(requestId.get());
    }

    if (body.isPresent()) {
      requestMessage.setBody(ByteString.copyFrom(body.get()));
    }

    if (headers != null) {
      requestMessage.addAllHeaders(headers);
    }

    SubProtocol.WebSocketMessage message
        = SubProtocol.WebSocketMessage.newBuilder()
                                      .setType(SubProtocol.WebSocketMessage.Type.REQUEST)
                                      .setRequest(requestMessage)
                                      .build();

    return new ProtobufWebSocketMessage(message);
  }

  @Override
  public WebSocketMessage createResponse(long requestId, int status, String messageString, List<String> headers, Optional<byte[]> body) {
    SubProtocol.WebSocketResponseMessage.Builder responseMessage =
        SubProtocol.WebSocketResponseMessage.newBuilder()
                                            .setId(requestId)
                                            .setStatus(status)
                                            .setMessage(messageString);

    if (body.isPresent()) {
      responseMessage.setBody(ByteString.copyFrom(body.get()));
    }

    if (headers != null) {
      responseMessage.addAllHeaders(headers);
    }

    SubProtocol.WebSocketMessage message =
        SubProtocol.WebSocketMessage.newBuilder()
                                    .setType(SubProtocol.WebSocketMessage.Type.RESPONSE)
                                    .setResponse(responseMessage)
                                    .build();

    return new ProtobufWebSocketMessage(message);
  }
}
