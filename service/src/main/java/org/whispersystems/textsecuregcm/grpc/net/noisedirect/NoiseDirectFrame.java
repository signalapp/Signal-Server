/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net.noisedirect;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;

public class NoiseDirectFrame extends DefaultByteBufHolder {

  static final byte VERSION = 0x00;

  private final FrameType frameType;

  public NoiseDirectFrame(final FrameType frameType, final ByteBuf data) {
    super(data);
    this.frameType = frameType;
  }

  public FrameType frameType() {
    return frameType;
  }

  public byte versionedFrameTypeByte() {
    final byte frameBits = frameType().getFrameBits();
    return (byte) ((NoiseDirectFrame.VERSION << 4) | frameBits);
  }


  public enum FrameType {
    /**
     * The payload is the initiator message for a Noise NK handshake. If established, the
     * session will be unauthenticated.
     */
    NK_HANDSHAKE((byte) 1),
    /**
     * The payload is the initiator message for a Noise IK handshake. If established, the
     * session will be authenticated.
     */
    IK_HANDSHAKE((byte) 2),
    /**
     * The payload is an encrypted noise packet.
     */
    DATA((byte) 3),
    /**
     * A frame sent before the connection is closed. The payload is a protobuf indicating why the connection is being
     * closed.
     */
    CLOSE((byte) 4);

    private final byte frameType;

    FrameType(byte frameType) {
      if (frameType != (0x0F & frameType)) {
        throw new IllegalStateException("Frame type must fit in 4 bits");
      }
      this.frameType = frameType;
    }

    public byte getFrameBits() {
      return frameType;
    }

    public boolean isHandshake() {
      return switch (this) {
        case IK_HANDSHAKE, NK_HANDSHAKE -> true;
        case DATA, CLOSE -> false;
      };
    }
  }
}
