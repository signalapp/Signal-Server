/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A TypedNoiseChannelDuplexHandler is a convenience {@link ChannelDuplexHandler} that can be inserted in a pipeline
 * after a successful websocket handshake. It expects inbound messages to be {@link BinaryWebSocketFrame}s and outbound
 * messages to be bytes.
 */
abstract class TypedNoiseChannelDuplexHandler extends ChannelDuplexHandler {

  private static final Logger log = LoggerFactory.getLogger(TypedNoiseChannelDuplexHandler.class);

  /**
   * Handle an inbound message. The frame will be automatically released after the method is finished running.
   *
   * @param context    The current {@link ChannelHandlerContext}
   * @param frameBytes A {@link ByteBuf} extracted from a {@link BinaryWebSocketFrame} that contains a complete noise
   *                   packet
   * @throws Exception
   */
  abstract void handleInbound(final ChannelHandlerContext context, ByteBuf frameBytes) throws Exception;

  /**
   * Handle an outbound byte message. The message will be automatically released after the method is finished running.
   *
   * @param context The current {@link ChannelHandlerContext}
   * @param bytes   The bytes to write
   * @throws Exception
   */
  abstract void handleOutbound(final ChannelHandlerContext context, final ByteBuf bytes,
      final ChannelPromise promise) throws Exception;

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
    try {
      if (message instanceof BinaryWebSocketFrame frame) {
        handleInbound(context, frame.content());
      } else {
        // Anything except binary WebSocket frames should have been filtered out of the pipeline by now; treat this as an
        // error
        throw new IllegalArgumentException("Unexpected message in pipeline: " + message);
      }
    } finally {
      ReferenceCountUtil.release(message);
    }
  }


  @Override
  public void write(final ChannelHandlerContext context, final Object message, final ChannelPromise promise)
      throws Exception {
    if (message instanceof ByteBuf serverResponse) {
      try {
        handleOutbound(context, serverResponse, promise);
      } finally {
        ReferenceCountUtil.release(serverResponse);
      }
    } else {
      if (!(message instanceof WebSocketFrame)) {
        // Downstream handlers may write WebSocket frames that don't need to be encrypted (e.g. "close" frames that
        // get issued in response to exceptions)
        log.warn("Unexpected object in pipeline: {}", message);
      }
      context.write(message, promise);
    }
  }
}
