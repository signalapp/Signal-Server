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
package org.whispersystems.websocket;

import com.google.common.annotations.VisibleForTesting;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.websocket.logging.WebsocketRequestLog;
import org.whispersystems.websocket.messages.InvalidMessageException;
import org.whispersystems.websocket.messages.WebSocketMessage;
import org.whispersystems.websocket.messages.WebSocketMessageFactory;
import org.whispersystems.websocket.messages.WebSocketRequestMessage;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import org.whispersystems.websocket.session.ContextPrincipal;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import org.whispersystems.websocket.setup.WebSocketConnectListener;

import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;


@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class WebSocketResourceProvider<T extends Principal> implements WebSocketListener {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketResourceProvider.class);

  private final Map<Long, CompletableFuture<WebSocketResponseMessage>> requestMap = new ConcurrentHashMap<>();

  private final T                                  authenticated;
  private final WebSocketMessageFactory            messageFactory;
  private final Optional<WebSocketConnectListener> connectListener;
  private final ApplicationHandler                 jerseyHandler;
  private final WebsocketRequestLog                requestLog;
  private final long                               idleTimeoutMillis;
  private final String                             remoteAddress;

  private Session                 session;
  private RemoteEndpoint          remoteEndpoint;
  private WebSocketSessionContext context;

  public WebSocketResourceProvider(String                             remoteAddress,
                                   ApplicationHandler                 jerseyHandler,
                                   WebsocketRequestLog                requestLog,
                                   T                                  authenticated,
                                   WebSocketMessageFactory            messageFactory,
                                   Optional<WebSocketConnectListener> connectListener,
                                   long                               idleTimeoutMillis)
  {
    this.remoteAddress     = remoteAddress;
    this.jerseyHandler     = jerseyHandler;
    this.requestLog        = requestLog;
    this.authenticated     = authenticated;
    this.messageFactory    = messageFactory;
    this.connectListener   = connectListener;
    this.idleTimeoutMillis = idleTimeoutMillis;
  }

  @Override
  public void onWebSocketConnect(Session session) {
    this.session        = session;
    this.remoteEndpoint = session.getRemote();
    this.context        = new WebSocketSessionContext(new WebSocketClient(session, remoteEndpoint, messageFactory, requestMap));
    this.context.setAuthenticated(authenticated);
    this.session.setIdleTimeout(idleTimeoutMillis);

    if (connectListener.isPresent()) {
      connectListener.get().onWebSocketConnect(this.context);
    }
  }

  @Override
  public void onWebSocketError(Throwable cause) {
    logger.debug("onWebSocketError", cause);
    close(session, 1011, "Server error");
  }

  @Override
  public void onWebSocketBinary(byte[] payload, int offset, int length) {
    try {
      WebSocketMessage webSocketMessage = messageFactory.parseMessage(payload, offset, length);

      switch (webSocketMessage.getType()) {
        case REQUEST_MESSAGE:
          handleRequest(webSocketMessage.getRequestMessage());
          break;
        case RESPONSE_MESSAGE:
          handleResponse(webSocketMessage.getResponseMessage());
          break;
        default:
          close(session, 1018, "Badly formatted");
          break;
      }
    } catch (InvalidMessageException e) {
      logger.debug("Parsing", e);
      close(session, 1018, "Badly formatted");
    }
  }

  @Override
  public void onWebSocketClose(int statusCode, String reason) {
    if (context != null) {
      context.notifyClosed(statusCode, reason);

      for (long requestId : requestMap.keySet()) {
        CompletableFuture<WebSocketResponseMessage> outstandingRequest = requestMap.remove(requestId);

        if (outstandingRequest != null) {
          outstandingRequest.completeExceptionally(new IOException("Connection closed!"));
        }
      }
    }
  }

  @Override
  public void onWebSocketText(String message) {
    logger.debug("onWebSocketText!");
  }

  private void handleRequest(WebSocketRequestMessage requestMessage) {
    ContainerRequest containerRequest = new ContainerRequest(null, URI.create(requestMessage.getPath()), requestMessage.getVerb(), new WebSocketSecurityContext(new ContextPrincipal(context)), new MapPropertiesDelegate(new HashMap<>()), null);

    for (Map.Entry<String, String> entry : requestMessage.getHeaders().entrySet()) {
      containerRequest.header(entry.getKey(), entry.getValue());
    }

    if (requestMessage.getBody().isPresent()) {
      containerRequest.setEntityStream(new ByteArrayInputStream(requestMessage.getBody().get()));
    }

    ByteArrayOutputStream                responseBody     = new ByteArrayOutputStream();
    CompletableFuture<ContainerResponse> responseFuture   = (CompletableFuture<ContainerResponse>) jerseyHandler.apply(containerRequest, responseBody);

    responseFuture.thenAccept(response -> {
      sendResponse(requestMessage, response, responseBody);
      requestLog.log(remoteAddress, containerRequest, response);
    }).exceptionally(exception -> {
      logger.warn("Websocket Error: " + requestMessage.getVerb() + " " + requestMessage.getPath() + "\n" + requestMessage.getBody(), exception);
      sendErrorResponse(requestMessage, Response.status(500).build());
      requestLog.log(remoteAddress, containerRequest, new ContainerResponse(containerRequest, Response.status(500).build()));
      return null;
    });
  }

  private void handleResponse(WebSocketResponseMessage responseMessage) {
    CompletableFuture<WebSocketResponseMessage> future = requestMap.remove(responseMessage.getRequestId());

    if (future != null) {
      future.complete(responseMessage);
    }
  }

  private void close(Session session, int status, String message) {
    session.close(status, message);
  }

  private void sendResponse(WebSocketRequestMessage requestMessage, ContainerResponse response, ByteArrayOutputStream responseBody) {
    if (requestMessage.hasRequestId()) {
      byte[] body = responseBody.toByteArray();

      if (body.length <= 0) {
        body = null;
      }

      byte[] responseBytes = messageFactory.createResponse(requestMessage.getRequestId(),
                                                           response.getStatus(),
                                                           response.getStatusInfo().getReasonPhrase(),
                                                           new LinkedList<>(),
                                                           Optional.ofNullable(body))
                                           .toByteArray();

      remoteEndpoint.sendBytesByFuture(ByteBuffer.wrap(responseBytes));
    }
  }

  private void sendErrorResponse(WebSocketRequestMessage requestMessage, Response error) {
    if (requestMessage.hasRequestId()) {
      List<String> headers = new LinkedList<>();

      for (String key : error.getStringHeaders().keySet()) {
        headers.add(key + ":" + error.getStringHeaders().getFirst(key));
      }

      WebSocketMessage response = messageFactory.createResponse(requestMessage.getRequestId(),
                                                                error.getStatus(),
                                                                "Error response",
                                                                headers,
                                                                Optional.empty());

      remoteEndpoint.sendBytesByFuture(ByteBuffer.wrap(response.toByteArray()));
    }
  }


  @VisibleForTesting
  WebSocketSessionContext getContext() {
    return context;
  }

}
