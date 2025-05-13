/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net.noisedirect;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.whispersystems.textsecuregcm.grpc.net.HandshakePattern;
import org.whispersystems.textsecuregcm.grpc.net.NoiseHandshakeException;
import org.whispersystems.textsecuregcm.grpc.net.NoiseHandshakeInit;

/**
 * Waits for a Handshake {@link NoiseDirectFrame} and then replaces itself with a {@link NoiseDirectDataFrameCodec} and
 * forwards the handshake frame along as a {@link NoiseHandshakeInit} message
 */
public class NoiseDirectHandshakeSelector extends ChannelInboundHandlerAdapter {

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    if (msg instanceof NoiseDirectFrame frame) {
      try {
        if (!(ctx.channel().remoteAddress() instanceof InetSocketAddress inetSocketAddress)) {
          throw new IOException("Could not determine remote address");
        }
        // We've received an inbound handshake frame. Pull the framing-protocol specific data the downstream handler
        // needs into a NoiseHandshakeInit message and forward that along
        final NoiseHandshakeInit handshakeMessage = new NoiseHandshakeInit(inetSocketAddress.getAddress(),
            switch (frame.frameType()) {
              case DATA -> throw new NoiseHandshakeException("First message must have handshake frame type");
              case CLOSE -> throw new IllegalStateException("Close frames should not reach handshake selector");
              case IK_HANDSHAKE -> HandshakePattern.IK;
              case NK_HANDSHAKE -> HandshakePattern.NK;
            }, frame.content());

        // Subsequent inbound messages and outbound should be data type frames or close frames. Inbound data frames
        // should be unwrapped and forwarded to the noise handler, outbound buffers should be wrapped and forwarded
        // for network serialization. Note that we need to install the Data frame handler before firing the read,
        // because we may receive an outbound message from the noiseHandler
        ctx.pipeline().replace(ctx.name(), null, new NoiseDirectDataFrameCodec());
        ctx.fireChannelRead(handshakeMessage);
      } catch (Exception e) {
        ReferenceCountUtil.release(msg);
        throw e;
      }
    } else {
      ctx.fireChannelRead(msg);
    }
  }
}
