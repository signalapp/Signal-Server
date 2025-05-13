/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net.noisedirect;

import com.southernstorm.noise.protocol.Noise;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import org.whispersystems.textsecuregcm.grpc.net.NoiseHandshakeException;

/**
 * Handles conversion between bytes on the wire and {@link NoiseDirectFrame}s. This handler assumes that inbound bytes
 * have already been framed using a {@link io.netty.handler.codec.LengthFieldBasedFrameDecoder}
 */
public class NoiseDirectFrameCodec extends ChannelDuplexHandler {

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    if (msg instanceof ByteBuf byteBuf) {
      try {
        ctx.fireChannelRead(deserialize(byteBuf));
      } catch (Exception e) {
        ReferenceCountUtil.release(byteBuf);
        throw e;
      }
    } else {
      ctx.fireChannelRead(msg);
    }
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof NoiseDirectFrame noiseDirectFrame) {
      try {
        // Serialize the frame into a newly allocated direct buffer. Since this is the last handler before the
        // network, nothing should have to make another copy of this. If later another layer is added, it may be more
        // efficient to reuse the input buffer (typically not direct) by using a composite byte buffer
        final ByteBuf serialized = serialize(ctx, noiseDirectFrame);
        ctx.writeAndFlush(serialized, promise);
      } finally {
        ReferenceCountUtil.release(noiseDirectFrame);
      }
    } else {
      ctx.write(msg, promise);
    }
  }

  private ByteBuf serialize(
      final ChannelHandlerContext ctx,
      final NoiseDirectFrame noiseDirectFrame) {
    if (noiseDirectFrame.content().readableBytes() > Noise.MAX_PACKET_LEN) {
      throw new IllegalStateException("Payload too long: " + noiseDirectFrame.content().readableBytes());
    }

    // 1 version/frametype byte, 2 length bytes, content
    final ByteBuf byteBuf = ctx.alloc().buffer(1 + 2 + noiseDirectFrame.content().readableBytes());

    byteBuf.writeByte(noiseDirectFrame.versionedFrameTypeByte());
    byteBuf.writeShort(noiseDirectFrame.content().readableBytes());
    byteBuf.writeBytes(noiseDirectFrame.content());
    return byteBuf;
  }

  private NoiseDirectFrame deserialize(final ByteBuf byteBuf) throws Exception {
    final byte versionAndFrameByte = byteBuf.readByte();
    final int version = (versionAndFrameByte & 0xF0) >> 4;
    if (version != NoiseDirectFrame.VERSION) {
      throw new NoiseHandshakeException("Invalid NoiseDirect version: " + version);
    }
    final byte frameTypeBits = (byte) (versionAndFrameByte & 0x0F);
    final NoiseDirectFrame.FrameType frameType = switch (frameTypeBits) {
      case 1 -> NoiseDirectFrame.FrameType.NK_HANDSHAKE;
      case 2 -> NoiseDirectFrame.FrameType.IK_HANDSHAKE;
      case 3 -> NoiseDirectFrame.FrameType.DATA;
      case 4 -> NoiseDirectFrame.FrameType.CLOSE;
      default -> throw new NoiseHandshakeException("Invalid NoiseDirect frame type: " + frameTypeBits);
    };

    final int length = Short.toUnsignedInt(byteBuf.readShort());
    if (length != byteBuf.readableBytes()) {
      throw new IllegalArgumentException(
          "Payload length did not match remaining buffer, should have been guaranteed by a previous handler");
    }
    return new NoiseDirectFrame(frameType, byteBuf.readSlice(length));
  }
}
