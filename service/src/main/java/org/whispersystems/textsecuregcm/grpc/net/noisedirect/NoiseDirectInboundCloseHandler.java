/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net.noisedirect;

import io.micrometer.core.instrument.Metrics;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;


/**
 * Watches for inbound close frames and closes the connection in response
 */
public class NoiseDirectInboundCloseHandler extends ChannelInboundHandlerAdapter {
  private static String CLIENT_CLOSE_COUNTER_NAME = MetricsUtil.name(NoiseDirectInboundCloseHandler.class, "clientClose");
  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    if (msg instanceof NoiseDirectFrame ndf && ndf.frameType() == NoiseDirectFrame.FrameType.CLOSE) {
      try {
        final NoiseDirectProtos.CloseReason closeReason = NoiseDirectProtos.CloseReason
            .parseFrom(ByteBufUtil.getBytes(ndf.content()));

        Metrics.counter(CLIENT_CLOSE_COUNTER_NAME, "reason", closeReason.getCode().name()).increment();
      } finally {
        ReferenceCountUtil.release(msg);
        ctx.close();
      }
    } else {
      ctx.fireChannelRead(msg);
    }
  }
}
