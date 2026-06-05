/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.signal.integration;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.websocket.messages.WebSocketMessage;
import org.whispersystems.websocket.messages.WebSocketMessageFactory;
import org.whispersystems.websocket.messages.WebSocketRequestMessage;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;

public class WebsocketClientSession implements Session.Listener.AutoDemanding {

  private static final Logger log = LoggerFactory.getLogger(WebsocketClientSession.class);
  private final WebSocketMessageFactory messageFactory = new ProtobufWebSocketMessageFactory();
  private final AtomicLong requestId = new AtomicLong();
  private final ConcurrentHashMap<Long, CompletableFuture<WebSocketResponseMessage>> responseFutures = new ConcurrentHashMap<>();
  private final List<MessageProtos.Envelope> receivedEnvelopes = new CopyOnWriteArrayList<>();
  private final CompletableFuture<Session> opened = new CompletableFuture<>();
  private final CompletableFuture<Void> queueEmpty = new CompletableFuture<>();
  private final CompletableFuture<Integer> closed = new CompletableFuture<>();

  @Override
  public void onWebSocketOpen(final Session session) {
    opened.complete(session);
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
            throw new IllegalArgumentException("Received response with no matching request: {}" + response.getRequestId());
          }
          future.complete(response);
        }
        default -> throw new IllegalStateException("Unexpected message type: " + message.getType());
      }
      callback.succeed();
    } catch (final Exception e) {
      log.warn("Failed to process message received over the websocket", e);
      callback.fail(e);
      opened.join().close(1006, e.getMessage(), Callback.NOOP);
    }
  }

  @Override
  public void onWebSocketClose(final int statusCode, final String reason, final Callback callback) {
    log.info("Received websocket close: {}", statusCode);
    closed.complete(statusCode);
    final IOException exception = new IOException("WebSocket closed: " + statusCode + " " + reason);
    responseFutures.values()
        .forEach(f -> f.completeExceptionally(exception));
    responseFutures.clear();
    if (!queueEmpty.isDone()) {
      queueEmpty.completeExceptionally(exception);
    }
    callback.succeed();
  }

  public <T> WebSocketResponseMessage sendRequest(
      final String verb,
      final String path,
      final List<String> headers,
      final T body) {
    final Session session = opened.join();
    final long id = requestId.incrementAndGet();
    final CompletableFuture<WebSocketResponseMessage> future = new CompletableFuture<>();
    responseFutures.put(id, future);
    final Optional<byte[]> maybeBody = Optional.ofNullable(body).map(Operations::encodeJsonBody);
    final byte[] bytes = messageFactory.createRequest(Optional.of(id), verb, path, headers, maybeBody).toByteArray();
    session.sendBinary(ByteBuffer.wrap(bytes), Callback.from(() -> {}, throwable -> {
      if (responseFutures.remove(id) != null) {
        future.completeExceptionally(throwable);
      }
    }));
    return future.join();
  }

  public List<MessageProtos.Envelope> getReceivedEnvelopes() {
    return receivedEnvelopes;
  }

  public void waitForQueueEmpty() {
    queueEmpty.join();
  }

  public void close(final int closeCode) {
    final Session session = opened.join();
    session.close(closeCode, "client close", Callback.NOOP);
    closed.join();
  }

  private void acknowledge(WebSocketRequestMessage message) {
    final byte[] envelopeBytes = message.getBody()
        .orElseThrow(() -> new IllegalStateException("Messages should have a response body"));
    try {
      final MessageProtos.Envelope envelope = MessageProtos.Envelope.parseFrom(envelopeBytes);
      receivedEnvelopes.add(envelope);
      final Session session = opened.join();
      final WebSocketMessage response = messageFactory.createResponse(message.getRequestId(), 200, "",
          Collections.emptyList(), Optional.empty());
      session.sendBinary(ByteBuffer.wrap(response.toByteArray()), Callback.NOOP);

    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }

  public static <R> R decode(Class<R> expectedType, WebSocketResponseMessage message) {
    try {
      return SystemMapper.jsonMapper()
          .readValue(message.getBody().orElseThrow(() -> new IllegalStateException("No response body")), expectedType);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
