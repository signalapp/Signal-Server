/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import java.util.concurrent.atomic.AtomicLong;

@ChannelHandler.Sharable
public class OmnibusConnectionCounterHandler extends ChannelInboundHandlerAdapter {
  private final AtomicLong openConnections;
  private final Counter acceptedConnectionsCounter =
      Metrics.counter(MetricsUtil.name(OmnibusConnectionCounterHandler.class, "connectionsAccepted"));

  public OmnibusConnectionCounterHandler() {
    openConnections =
        Metrics.gauge(MetricsUtil.name(OmnibusConnectionCounterHandler.class, "openConnections"), new AtomicLong());
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    acceptedConnectionsCounter.increment();
    openConnections.incrementAndGet();
    super.channelRegistered(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    openConnections.decrementAndGet();
    super.channelInactive(ctx);
  }
}
