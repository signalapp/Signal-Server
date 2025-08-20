/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net.websocket;

import io.micrometer.core.instrument.Metrics;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

public class WebSocketCloseMetricHandler extends ChannelDuplexHandler {

  private static String CLIENT_CLOSE_COUNTER_NAME = MetricsUtil.name(WebSocketCloseMetricHandler.class, "clientClose");
  private static String SERVER_CLOSE_COUNTER_NAME = MetricsUtil.name(WebSocketCloseMetricHandler.class, "serverClose");


  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    if (msg instanceof CloseWebSocketFrame closeFrame) {
      Metrics.counter(CLIENT_CLOSE_COUNTER_NAME, "closeCode", validatedCloseCode(closeFrame.statusCode())).increment();
    }
    ctx.fireChannelRead(msg);
  }


  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof CloseWebSocketFrame closeFrame) {
      Metrics.counter(SERVER_CLOSE_COUNTER_NAME, "closeCode", Integer.toString(closeFrame.statusCode())).increment();
    }
    ctx.write(msg, promise);
  }

  private static String validatedCloseCode(int closeCode) {

    if (closeCode >= 1000 && closeCode <= 1015) {
      // RFC-6455 pre-defined status codes
      return Integer.toString(closeCode);
    } else if (closeCode >= 4000 && closeCode <= 4100) {
      // Application status codes
      return Integer.toString(closeCode);
    } else {
      return "unknown";
    }
  }

}
