/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import io.micrometer.core.instrument.Metrics;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http2.Http2StreamFrame;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

/// Writes all inbound H2 frames to [this#peerStream], renumbering the inbound H2 stream-id for peer H2 stream
public class H2FrameProxyHandler extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(H2FrameProxyHandler.class);
  private static final String WRITABILITY_CHANGED_COUNTER_NAME = MetricsUtil.name(H2FrameProxyHandler.class, "writabilityChanged");

  private final Channel peerStream;
  private final String proxyNameTag;

  // If we fail to write to the peerStream, we want to close the inbound channel. Rather than allocate a new listener
  // that captures the inbound ChannelHandlerContext on every message, we capture the ChannelHandlerContext in
  // handlerAdded and use it on all forwarded writes. This would not work if we attached this handler to more than
  // one channel, but we already have a designated peerStream so this handler is fundamentally single-channel.
  private ChannelFutureListener closeInboundOnPeerFailure = null;

  public H2FrameProxyHandler(final Channel peerStream, final String proxyNameTag) {
    this.peerStream = peerStream;
    this.proxyNameTag = proxyNameTag;
  }

  @Override
  public void handlerAdded(final ChannelHandlerContext ctx) {
    closeInboundOnPeerFailure = f -> {
      if (!f.isSuccess()) {
        ctx.close();
      }
    };

    // When the peer stream we are forwarding to becomes unwritable/writable, stop/start reading from the source stream.
    // This prevents us from reading from the source stream as fast as we can just to buffer requests for the peer
    // stream.
    peerStream.pipeline().addLast(new ChannelInboundHandlerAdapter() {
      @Override
      public void channelWritabilityChanged(final ChannelHandlerContext peerCtx) throws Exception {
        Metrics.counter(WRITABILITY_CHANGED_COUNTER_NAME,
            "isWritable", Boolean.toString(peerCtx.channel().isWritable()),
                "proxy", proxyNameTag)
            .increment();
        ctx.channel().config().setAutoRead(peerStream.isWritable());
        super.channelWritabilityChanged(peerCtx);
      }
    });
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    logger.trace("Received frame {}", msg);
    if (!(msg instanceof Http2StreamFrame streamFrame)) {
      logger.error("Received unexpected frame {}", msg);
      ReferenceCountUtil.release(msg);
      ctx.close();
      return;
    }

    // Clear the stream-id on this frame, so netty will associate it with the peerStream's stream-id. The inbound
    // frame has a stream-id associated with the inbound connection. This will not match the stream-id of the peer
    // stream we are forwarding the frames to. If the stream-id on a frame is not set, netty handles sending the
    // stream-id on the frame to the target stream's stream-id.
    streamFrame.stream(null);

    peerStream.writeAndFlush(streamFrame).addListener(closeInboundOnPeerFailure);
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) {
    peerStream.close();
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    logger.warn("Exception proxying frames", cause);
    ctx.close();
  }
}
