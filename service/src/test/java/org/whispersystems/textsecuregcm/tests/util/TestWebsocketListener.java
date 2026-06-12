/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.tests.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import com.google.protobuf.InvalidProtocolBufferException;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.websocket.messages.WebSocketMessage;
import org.whispersystems.websocket.messages.WebSocketMessageFactory;
import org.whispersystems.websocket.messages.WebSocketRequestMessage;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;

public class TestWebsocketListener implements Session.Listener.AutoDemanding {

  private static final Logger log = LoggerFactory.getLogger(TestWebsocketListener.class);
  private final AtomicLong requestId = new AtomicLong();
  protected final CompletableFuture<Session> started = new CompletableFuture<>();
  private final CompletableFuture<Void> queueEmpty = new CompletableFuture<>();
  private final CompletableFuture<Integer> closed = new CompletableFuture<>();
  private final List<MessageProtos.Envelope> receivedEnvelopes = new CopyOnWriteArrayList<>();
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

  public CompletableFuture<Void> queueEmptyFuture() {
    return queueEmpty;
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

  public List<MessageProtos.Envelope> getReceivedEnvelopes() {
    return receivedEnvelopes;
  }

  @Override
  public void onWebSocketBinary(final ByteBuffer payload, final Callback callback) {
    try {
      final WebSocketMessage message = messageFactory.parseMessage(payload);
      switch (message.getType()) {
        case REQUEST_MESSAGE -> {
          log.info("received request message {} {}", message.getRequestMessage().getVerb(), message.getRequestMessage().getPath());
          switch (message.getRequestMessage().getPath()) {
            case "/api/v1/message" -> acknowledge(message.getRequestMessage());
            case "/api/v1/queue/empty" -> queueEmpty.complete(null);
            default -> throw new IllegalStateException("Unexpected path: " + message.getRequestMessage().getPath());
          }
        }
        case RESPONSE_MESSAGE -> {
          final WebSocketResponseMessage response = message.getResponseMessage();
          log.info("received response message {}", response.getStatus());
          final CompletableFuture<WebSocketResponseMessage> future = responseFutures.remove(response.getRequestId());
          if (future == null) {
            throw new IllegalArgumentException("Received response with no matching request: " + response.getRequestId());
          }
          future.complete(response);
        }
        default -> throw new IllegalStateException("Unexpected message type: " + message.getType());
      }
      callback.succeed();
    } catch (final Exception e) {
      log.warn("Failed to process message received over the websocket", e);
      callback.fail(e);
      started.join().close(1006, e.getMessage(), Callback.NOOP);
    }
  }

  private void acknowledge(WebSocketRequestMessage message) {
    final byte[] envelopeBytes = message.getBody()
        .orElseThrow(() -> new UncheckedIOException(new IOException("Messages should have a body")));
    try {
      final MessageProtos.Envelope envelope = MessageProtos.Envelope.parseFrom(envelopeBytes);
      receivedEnvelopes.add(envelope);
      final Session session = started.join();
      final WebSocketMessage response = messageFactory.createResponse(message.getRequestId(), 200, "",
          Collections.emptyList(), Optional.empty());
      session.sendBinary(ByteBuffer.wrap(response.toByteArray()), Callback.NOOP);

    } catch (InvalidProtocolBufferException e) {
      throw new UncheckedIOException(e);
    }
  }
}
