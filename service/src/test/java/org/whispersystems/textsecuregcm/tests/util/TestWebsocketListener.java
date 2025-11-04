/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.tests.util;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.whispersystems.websocket.messages.WebSocketMessage;
import org.whispersystems.websocket.messages.WebSocketMessageFactory;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;

public class TestWebsocketListener implements Session.Listener.AutoDemanding {

  private final AtomicLong requestId = new AtomicLong();
  private final CompletableFuture<Session> started = new CompletableFuture<>();
  private final CompletableFuture<Integer> closed = new CompletableFuture<>();
  private final ConcurrentHashMap<Long, CompletableFuture<WebSocketResponseMessage>> responseFutures = new ConcurrentHashMap<>();
  protected final WebSocketMessageFactory messageFactory;

  public TestWebsocketListener() {
    this.messageFactory = new ProtobufWebSocketMessageFactory();
  }


  @Override
  public void onWebSocketOpen(final Session session) {
    started.complete(session);

  }

  @Override
  public void onWebSocketClose(int statusCode, String reason) {
    closed.complete(statusCode);
  }

  public CompletableFuture<Integer> closeFuture() {
    return closed;
  }

  public CompletableFuture<WebSocketResponseMessage> doGet(final String requestPath) {
    return sendRequest(requestPath, "GET", List.of("Accept: application/json"), Optional.empty());
  }

  public CompletableFuture<WebSocketResponseMessage> sendRequest(
      final String requestPath,
      final String verb,
      final List<String> headers,
      final Optional<byte[]> body) {
    return started.thenCompose(session -> {
      final long id = requestId.incrementAndGet();
      final CompletableFuture<WebSocketResponseMessage> future = new CompletableFuture<>();
      responseFutures.put(id, future);
      final byte[] requestBytes = messageFactory.createRequest(
          Optional.of(id), verb, requestPath, headers, body).toByteArray();
      session.sendBinary(ByteBuffer.wrap(requestBytes), Callback.NOOP);
      return future;
    });
  }

  @Override
  public void onWebSocketBinary(final ByteBuffer payload, final Callback callback) {
    try {
      WebSocketMessage webSocketMessage = messageFactory.parseMessage(payload);
      if (Objects.requireNonNull(webSocketMessage.getType()) == WebSocketMessage.Type.RESPONSE_MESSAGE) {
        responseFutures.get(webSocketMessage.getResponseMessage().getRequestId())
            .complete(webSocketMessage.getResponseMessage());
      } else {
        throw new RuntimeException("Unexpected message type: " + webSocketMessage.getType());
      }
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
