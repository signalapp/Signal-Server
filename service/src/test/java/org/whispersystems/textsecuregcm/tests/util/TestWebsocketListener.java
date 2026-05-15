/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.tests.util;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.whispersystems.websocket.messages.WebSocketMessage;
import org.whispersystems.websocket.messages.WebSocketMessageFactory;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TestWebsocketListener implements WebSocketListener {

  private final AtomicLong requestId = new AtomicLong();
  private final CompletableFuture<Session> started = new CompletableFuture<>();
  private final CompletableFuture<Integer> closed = new CompletableFuture<>();
  private final ConcurrentHashMap<Long, CompletableFuture<WebSocketResponseMessage>> responseFutures = new ConcurrentHashMap<>();
  protected final WebSocketMessageFactory messageFactory;

  public TestWebsocketListener() {
    this.messageFactory = new ProtobufWebSocketMessageFactory();
  }


  @Override
  public void onWebSocketConnect(final Session session) {
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
      try {
        session.getRemote().sendBytes(ByteBuffer.wrap(requestBytes));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return future;
    });
  }

  @Override
  public void onWebSocketBinary(final byte[] payload, final int offset, final int length) {
    try {
      WebSocketMessage webSocketMessage = messageFactory.parseMessage(payload, offset, length);
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
