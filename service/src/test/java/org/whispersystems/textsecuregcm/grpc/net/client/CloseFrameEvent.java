/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net.client;

import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.whispersystems.textsecuregcm.grpc.net.noisedirect.NoiseDirectProtos;

public record CloseFrameEvent(CloseReason closeReason, CloseInitiator closeInitiator, String reason) {

  public enum CloseReason {
    SERVER_CLOSED,
    NOISE_ERROR,
    NOISE_HANDSHAKE_ERROR,
    AUTHENTICATION_ERROR,
    INTERNAL_SERVER_ERROR,
    UNKNOWN
  }

  public enum CloseInitiator {
    SERVER,
    CLIENT
  }

  public static CloseFrameEvent fromWebsocketCloseFrame(
      CloseWebSocketFrame closeWebSocketFrame,
      CloseInitiator closeInitiator) {
    final CloseReason code = switch (closeWebSocketFrame.statusCode()) {
      case 4003 -> CloseReason.NOISE_ERROR;
      case 4001 -> CloseReason.NOISE_HANDSHAKE_ERROR;
      case 4002 -> CloseReason.AUTHENTICATION_ERROR;
      case 1011 -> CloseReason.INTERNAL_SERVER_ERROR;
      case 1012 -> CloseReason.SERVER_CLOSED;
      default -> CloseReason.UNKNOWN;
    };
    return new CloseFrameEvent(code, closeInitiator, closeWebSocketFrame.reasonText());
  }

  public static CloseFrameEvent fromNoiseDirectErrorFrame(
      NoiseDirectProtos.Error noiseDirectError,
      CloseInitiator closeInitiator) {
    final CloseReason code = switch (noiseDirectError.getType()) {
      case HANDSHAKE_ERROR -> CloseReason.NOISE_HANDSHAKE_ERROR;
      case ENCRYPTION_ERROR -> CloseReason.NOISE_ERROR;
      case UNAVAILABLE -> CloseReason.SERVER_CLOSED;
      case INTERNAL_ERROR -> CloseReason.INTERNAL_SERVER_ERROR;
      case AUTHENTICATION_ERROR -> CloseReason.AUTHENTICATION_ERROR;
      case UNRECOGNIZED, UNSPECIFIED -> CloseReason.UNKNOWN;
    };
    return new CloseFrameEvent(code, closeInitiator, noiseDirectError.getMessage());
  }
}
