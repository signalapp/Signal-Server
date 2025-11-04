/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.messages;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public interface WebSocketMessageFactory {

  WebSocketMessage parseMessage(ByteBuffer serialized)
      throws InvalidMessageException;

  WebSocketMessage createRequest(Optional<Long> requestId,
      String verb, String path,
      List<String> headers,
      Optional<byte[]> body);

  WebSocketMessage createResponse(long requestId, int status, String message,
      List<String> headers,
      Optional<byte[]> body);

}
