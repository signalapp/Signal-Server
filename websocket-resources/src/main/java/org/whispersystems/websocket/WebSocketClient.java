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
package org.whispersystems.websocket;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.websocket.messages.WebSocketMessage;
import org.whispersystems.websocket.messages.WebSocketMessageFactory;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class WebSocketClient {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketClient.class);

  private final Session                                             session;
  private final RemoteEndpoint                                      remoteEndpoint;
  private final WebSocketMessageFactory                             messageFactory;
  private final Map<Long, SettableFuture<WebSocketResponseMessage>> pendingRequestMapper;

  public WebSocketClient(Session session, RemoteEndpoint remoteEndpoint,
                         WebSocketMessageFactory messageFactory,
                         Map<Long, SettableFuture<WebSocketResponseMessage>> pendingRequestMapper)
  {
    this.session              = session;
    this.remoteEndpoint       = remoteEndpoint;
    this.messageFactory       = messageFactory;
    this.pendingRequestMapper = pendingRequestMapper;
  }

  public ListenableFuture<WebSocketResponseMessage> sendRequest(String verb, String path,
                                                                List<String> headers,
                                                                Optional<byte[]> body)
  {
    final long                                     requestId = generateRequestId();
    final SettableFuture<WebSocketResponseMessage> future    = SettableFuture.create();

    pendingRequestMapper.put(requestId, future);

    WebSocketMessage requestMessage = messageFactory.createRequest(Optional.of(requestId), verb, path, headers, body);

    try {
      remoteEndpoint.sendBytes(ByteBuffer.wrap(requestMessage.toByteArray()), new WriteCallback() {
        @Override
        public void writeFailed(Throwable x) {
          logger.debug("Write failed", x);
          pendingRequestMapper.remove(requestId);
          future.setException(x);
        }

        @Override
        public void writeSuccess() {}
      });
    } catch (WebSocketException e) {
      logger.debug("Write", e);
      pendingRequestMapper.remove(requestId);
      future.setException(e);
    }

    return future;
  }

  public void close(int code, String message) {
    session.close(code, message);
  }

  public void hardDisconnectQuietly() {
    try {
      session.disconnect();
    } catch (IOException e) {
      // quietly we said
    }
  }

  private long generateRequestId() {
    return Math.abs(new SecureRandom().nextLong());
  }

}
