/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net.noisedirect;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import org.whispersystems.textsecuregcm.grpc.net.NoiseException;

/**
 * In the inbound direction, this handler strips the NoiseDirectFrame wrapper we read off the wire and then forwards the
 * noise packet to the noise layer as a {@link ByteBuf} for decryption.
 * <p>
 * In the outbound direction, this handler wraps encrypted noise packet {@link ByteBuf}s in a NoiseDirectFrame wrapper
 * so it can be wire serialized. This handler assumes the first outbound message received will correspond to the
 * handshake response, and then the subsequent messages are all data frame payloads.
 */
public class NoiseDirectDataFrameCodec extends ChannelDuplexHandler {

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    if (msg instanceof NoiseDirectFrame frame) {
      if (frame.frameType() != NoiseDirectFrame.FrameType.DATA) {
        ReferenceCountUtil.release(msg);
        throw new NoiseException("Invalid frame type received (expected DATA): " + frame.frameType());
      }
      ctx.fireChannelRead(frame.content());
    } else {
      ctx.fireChannelRead(msg);
    }
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof ByteBuf bb) {
      ctx.write(new NoiseDirectFrame(NoiseDirectFrame.FrameType.DATA, bb), promise);
    } else {
      ctx.write(msg, promise);
    }
  }
}
