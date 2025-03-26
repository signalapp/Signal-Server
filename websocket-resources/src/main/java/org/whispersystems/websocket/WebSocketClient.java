/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket;

import com.google.common.net.HttpHeaders;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.api.exceptions.WebSocketException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.websocket.messages.WebSocketMessage;
import org.whispersystems.websocket.messages.WebSocketMessageFactory;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class WebSocketClient {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketClient.class);
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  private final Session session;
  private final RemoteEndpoint remoteEndpoint;
  private final WebSocketMessageFactory messageFactory;
  private final Map<Long, CompletableFuture<WebSocketResponseMessage>> pendingRequestMapper;
  private final Instant created;

  public WebSocketClient(Session session, RemoteEndpoint remoteEndpoint, WebSocketMessageFactory messageFactory,
                         Map<Long, CompletableFuture<WebSocketResponseMessage>> pendingRequestMapper) {
    this.session = session;
    this.remoteEndpoint = remoteEndpoint;
    this.messageFactory = messageFactory;
    this.pendingRequestMapper = pendingRequestMapper;
    this.created = Instant.now();
  }

  public CompletableFuture<WebSocketResponseMessage> sendRequest(String verb, String path,
                                                                 List<String> headers,
                                                                 Optional<byte[]> body)
  {
    final long                                        requestId = generateRequestId();
    final CompletableFuture<WebSocketResponseMessage> future    = new CompletableFuture<>();

    pendingRequestMapper.put(requestId, future);

    WebSocketMessage requestMessage = messageFactory.createRequest(Optional.of(requestId), verb, path, headers, body);

    try {
      remoteEndpoint.sendBytes(ByteBuffer.wrap(requestMessage.toByteArray()), new WriteCallback() {
        @Override
        public void writeFailed(Throwable x) {
          logger.debug("Write failed", x);
          pendingRequestMapper.remove(requestId);
          future.completeExceptionally(x);
        }
      });
    } catch (WebSocketException e) {
      logger.debug("Write", e);
      pendingRequestMapper.remove(requestId);
      future.completeExceptionally(e);
    }

    return future;
  }

  public String getUserAgent() {
    return session.getUpgradeRequest().getHeader(HttpHeaders.USER_AGENT);
  }

  public Instant getCreated() {
    return this.created;
  }

  public boolean isOpen() {
    return session.isOpen();
  }

  public void close(final int code, final String message) {
    session.close(code, message, new WriteCallback() {
      @Override
      public void writeFailed(final Throwable throwable) {
        try {
          session.disconnect();
        } catch (final Exception e) {
          logger.warn("Failed to disconnect session", e);
        }
      }
    });
  }

  public boolean shouldDeliverStories() {
    String value = session.getUpgradeRequest().getHeader(WebsocketHeaders.X_SIGNAL_RECEIVE_STORIES);
    return WebsocketHeaders.parseReceiveStoriesHeader(value);
  }

  private long generateRequestId() {
    return Math.abs(SECURE_RANDOM.nextLong());
  }
}
