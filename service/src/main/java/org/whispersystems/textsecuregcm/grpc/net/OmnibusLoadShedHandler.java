/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http2.DefaultHttp2GoAwayFrame;
import io.netty.util.ReferenceCountUtil;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

@ChannelHandler.Sharable
public class OmnibusLoadShedHandler extends ChannelInboundHandlerAdapter {
  // Error code to put in GOAWAY frames that indicate load-shedding. 0x40
  // is an arbitrary non-reserved code per https://datatracker.ietf.org/doc/html/rfc7540#section-7
  @VisibleForTesting
  static final int LOAD_SHED_ERROR_CODE = 0x40;

  private static final Counter LOAD_SHED_COUNTER =
      Metrics.counter(MetricsUtil.name(OmnibusLoadShedHandler.class, "loadShed"));

  @Override
  public void handlerAdded(final ChannelHandlerContext ctx) {
    LOAD_SHED_COUNTER.increment();
    final DefaultHttp2GoAwayFrame goAwayFrame = new DefaultHttp2GoAwayFrame(LOAD_SHED_ERROR_CODE);
    ctx.writeAndFlush(goAwayFrame).addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    // Drop any attempts to open streams
    ReferenceCountUtil.release(msg);
  }

}
