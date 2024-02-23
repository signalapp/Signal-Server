package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;

enum ApplicationWebSocketCloseReason {
  NOISE_HANDSHAKE_ERROR(4001),
  CLIENT_AUTHENTICATION_ERROR(4002),
  NOISE_ENCRYPTION_ERROR(4003),
  REAUTHENTICATION_REQUIRED(4004);

  private final int statusCode;

  ApplicationWebSocketCloseReason(final int statusCode) {
    this.statusCode = statusCode;
  }

  public int getStatusCode() {
    return statusCode;
  }

  WebSocketCloseStatus toWebSocketCloseStatus(final String reason) {
    return new WebSocketCloseStatus(statusCode, reason);
  }
}
