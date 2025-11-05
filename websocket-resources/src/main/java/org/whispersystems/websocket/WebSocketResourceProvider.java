/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import com.google.protobuf.UninitializedMessageException;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.api.exceptions.MessageTooLargeException;
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


@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class WebSocketResourceProvider<T extends Principal> implements WebSocketListener {

  /**
   * A static exception instance passed to outstanding requests (via {@code completeExceptionally} in
   * {@link #onWebSocketClose(int, String)}
   */
  public static final IOException CONNECTION_CLOSED_EXCEPTION = new IOException("Connection closed!");
  private static final Logger logger = LoggerFactory.getLogger(WebSocketResourceProvider.class);

  private final Map<Long, CompletableFuture<WebSocketResponseMessage>> requestMap = new ConcurrentHashMap<>();

  private final Optional<T> reusableAuth;
  private final WebSocketMessageFactory messageFactory;
  private final Optional<WebSocketConnectListener> connectListener;
  private final ApplicationHandler jerseyHandler;
  private final WebsocketRequestLog requestLog;
  private final Duration idleTimeout;
  private final String remoteAddress;
  private final String remoteAddressPropertyName;

  private Session session;
  private RemoteEndpoint remoteEndpoint;
  private WebSocketSessionContext context;

  private static final Set<String> EXCLUDED_UPGRADE_REQUEST_HEADERS = Set.of("connection", "upgrade");

  public WebSocketResourceProvider(String remoteAddress,
      String remoteAddressPropertyName,
      ApplicationHandler jerseyHandler,
      WebsocketRequestLog requestLog,
      Optional<T> authenticated,
      WebSocketMessageFactory messageFactory,
      Optional<WebSocketConnectListener> connectListener,
      Duration idleTimeout) {
    this.remoteAddress = remoteAddress;
    this.remoteAddressPropertyName = remoteAddressPropertyName;
    this.jerseyHandler = jerseyHandler;
    this.requestLog = requestLog;
    this.reusableAuth = authenticated;
    this.messageFactory = messageFactory;
    this.connectListener = connectListener;
    this.idleTimeout = idleTimeout;
  }

  @Override
  public void onWebSocketConnect(Session session) {
    this.session = session;
    this.remoteEndpoint = session.getRemote();
    this.context = new WebSocketSessionContext(
        new WebSocketClient(session, remoteEndpoint, messageFactory, requestMap));
    this.context.setAuthenticated(reusableAuth.orElse(null));
    this.session.setIdleTimeout(idleTimeout);

    connectListener.ifPresent(listener -> listener.onWebSocketConnect(this.context));
  }

  @Override
  public void onWebSocketError(Throwable cause) {
    logger.debug("onWebSocketError", cause);

    final int closeCode;
    final String message;
    if (cause instanceof MessageTooLargeException) {
      closeCode = 1009;
      message = "Frame too large";
    } else {
      closeCode = 1011;
      message = "Server error";
    }

    close(session, closeCode, message);
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
          close(session, 1007, "Badly formatted");
          break;
      }
    } catch (UninitializedMessageException | InvalidMessageException e) {
      logger.debug("Parsing", e);
      close(session, 1007, "Badly formatted");
    }
  }

  @Override
  public void onWebSocketClose(int statusCode, String reason) {
    if (context != null) {
      context.notifyClosed(statusCode, reason);

      for (long requestId : requestMap.keySet()) {
        CompletableFuture<WebSocketResponseMessage> outstandingRequest = requestMap.remove(requestId);

        if (outstandingRequest != null) {
          outstandingRequest.completeExceptionally(CONNECTION_CLOSED_EXCEPTION);
        }
      }
    }
  }

  @Override
  public void onWebSocketText(String message) {
    logger.debug("onWebSocketText!");
  }

  /**
   * The property name where {@link org.whispersystems.websocket.auth.WebsocketAuthValueFactoryProvider} can find an
   * authenticated principal that lives for the lifetime of the websocket
   */
  public static final String REUSABLE_AUTH_PROPERTY = WebSocketResourceProvider.class.getName() + ".reusableAuth";

  /**
   * The property name where request byte count is stored for metrics collection
   */
  public static final String REQUEST_LENGTH_PROPERTY = WebSocketResourceProvider.class.getName() + ".requestBytes";

  /**
   * The property name where response byte count is stored for metrics collection
   */
  public static final String RESPONSE_LENGTH_PROPERTY = WebSocketResourceProvider.class.getName() + ".responseBytes";

  private void handleRequest(WebSocketRequestMessage requestMessage) {
    ContainerRequest containerRequest = new ContainerRequest(null, URI.create(requestMessage.getPath()),
        requestMessage.getVerb(), new WebSocketSecurityContext(new ContextPrincipal(context)),
        new MapPropertiesDelegate(new HashMap<>()), jerseyHandler.getConfiguration());
    containerRequest.headers(getCombinedHeaders(session.getUpgradeRequest().getHeaders(), requestMessage.getHeaders()));

    final int requestBytes = requestMessage.getBody().map(body -> body.length).orElse(0);

    if (requestMessage.getBody().isPresent()) {
      containerRequest.setEntityStream(new ByteArrayInputStream(requestMessage.getBody().get()));
    }

    containerRequest.setProperty(remoteAddressPropertyName, remoteAddress);
    containerRequest.setProperty(REUSABLE_AUTH_PROPERTY, reusableAuth);
    containerRequest.setProperty(REQUEST_LENGTH_PROPERTY, requestBytes);

    ByteArrayOutputStream responseBody = new ByteArrayOutputStream();
    CompletableFuture<ContainerResponse> responseFuture = (CompletableFuture<ContainerResponse>) jerseyHandler.apply(
        containerRequest, responseBody);

    responseFuture
        .thenAccept(response -> {
          try {
            final int responseBytes = responseBody.size();
            containerRequest.setProperty(RESPONSE_LENGTH_PROPERTY, responseBytes);
            sendResponse(requestMessage, response, responseBody);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          requestLog.log(remoteAddress, containerRequest, response);
        })
        .exceptionally(exception -> {
          logger.warn("Websocket Error: " + requestMessage.getVerb() + " " + requestMessage.getPath() + "\n"
              + requestMessage.getBody(), exception);
          try {
            containerRequest.setProperty(RESPONSE_LENGTH_PROPERTY, 0);
            sendErrorResponse(requestMessage, Response.status(500).build());
          } catch (IOException e) {
            logger.warn("Failed to send error response", e);
          }
          requestLog.log(remoteAddress, containerRequest,
              new ContainerResponse(containerRequest, Response.status(500).build()));
          return null;
        });
  }

  @VisibleForTesting
  static Map<String, List<String>> getCombinedHeaders(final Map<String, List<String>> upgradeRequestHeaders, final Map<String, String> requestMessageHeaders) {
    final Map<String, List<String>> combinedHeaders = new HashMap<>();

    upgradeRequestHeaders.entrySet().stream()
        .filter(entry -> shouldIncludeUpgradeRequestHeader(entry.getKey()))
        .forEach(entry -> combinedHeaders.put(entry.getKey(), entry.getValue()));

    requestMessageHeaders.entrySet().stream()
        .filter(entry -> shouldIncludeRequestMessageHeader(entry.getKey()))
        .forEach(entry -> combinedHeaders.put(entry.getKey(), List.of(entry.getValue())));

    return combinedHeaders;
  }

  @VisibleForTesting
  static boolean shouldIncludeUpgradeRequestHeader(final String header) {
    return !EXCLUDED_UPGRADE_REQUEST_HEADERS.contains(header.toLowerCase()) && !header.toLowerCase().contains("websocket-");
  }

  @VisibleForTesting
  static boolean shouldIncludeRequestMessageHeader(final String header) {
    return !HttpHeaders.X_FORWARDED_FOR.equalsIgnoreCase(header.trim());
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

  private void sendResponse(WebSocketRequestMessage requestMessage, ContainerResponse response,
      ByteArrayOutputStream responseBody) throws IOException {
    if (requestMessage.hasRequestId()) {
      byte[] body = responseBody.toByteArray();
      response.getHeaders().putIfAbsent(HttpHeaders.CONTENT_LENGTH, List.of(body.length));
      if (body.length <= 0) {
        body = null;
      }

      byte[] responseBytes = messageFactory.createResponse(requestMessage.getRequestId(),
              response.getStatus(),
              response.getStatusInfo().getReasonPhrase(),
              getHeaderList(response.getStringHeaders()),
              Optional.ofNullable(body))
          .toByteArray();

      remoteEndpoint.sendBytes(ByteBuffer.wrap(responseBytes), WriteCallback.NOOP);
    }
  }

  private void sendErrorResponse(WebSocketRequestMessage requestMessage, Response error) throws IOException {
    if (requestMessage.hasRequestId()) {
      WebSocketMessage response = messageFactory.createResponse(requestMessage.getRequestId(),
          error.getStatus(),
          "Error response",
          getHeaderList(error.getStringHeaders()),
          Optional.empty());

      remoteEndpoint.sendBytes(ByteBuffer.wrap(response.toByteArray()), WriteCallback.NOOP);
    }
  }


  @VisibleForTesting
  WebSocketSessionContext getContext() {
    return context;
  }

  @VisibleForTesting
  static List<String> getHeaderList(final MultivaluedMap<String, String> headerMap) {
    final List<String> headers = new LinkedList<>();

    if (headerMap != null) {
      for (String key : headerMap.keySet()) {
        headers.add(key + ":" + headerMap.getFirst(key));
      }
    }

    return headers;
  }
}
