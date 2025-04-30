/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.net.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

/**
 * Extracts buffers from inbound BinaryWebsocketFrames before forwarding to a
 * {@link org.whispersystems.textsecuregcm.grpc.net.NoiseHandler} for decryption and wraps outbound encrypted noise
 * packet buffers in BinaryWebsocketFrames for writing through the websocket layer.
 */
public class WebsocketPayloadCodec extends ChannelDuplexHandler {

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (msg instanceof BinaryWebSocketFrame frame) {
      ctx.fireChannelRead(frame.content());
    } else {
      ctx.fireChannelRead(msg);
    }
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof ByteBuf bb) {
      ctx.write(new BinaryWebSocketFrame(bb), promise);
    } else {
      ctx.write(msg, promise);
    }
  }
}
