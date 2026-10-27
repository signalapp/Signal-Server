/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.collect.ImmutableSet;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.util.concurrent.EventExecutor;
import java.net.SocketAddress;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnpooledH2ConnectionFactory implements H2ConnectionFactory {
  private static final Logger logger = LoggerFactory.getLogger(UnpooledH2ConnectionFactory.class);

  private static final OmnibusExceptionHandler BACKEND_CONNECTION_EXCEPTION_HANDLER =
      new OmnibusExceptionHandler("backend-connection", List.of());
  private static final String BACKEND_CONNECT_TIMER_NAME = name(UnpooledH2ConnectionFactory.class,
      "backendConnectDuration");
  private static final String BACKEND_TAG = "backend";


  private final SocketAddress targetAddress;
  private final Class<? extends Channel> channelClass;
  private final EventLoopGroup eventLoopGroup;
  private final Set<EventExecutor> eventLoops;

  public UnpooledH2ConnectionFactory(final SocketAddress targetAddress, final EventLoopGroup eventLoopGroup) {
    this.targetAddress = targetAddress;
    this.channelClass = targetAddress instanceof LocalAddress ? LocalChannel.class : NioSocketChannel.class;
    this.eventLoopGroup = eventLoopGroup;
    this.eventLoops = ImmutableSet.copyOf(eventLoopGroup);
  }

  @Override
  public ChannelFuture getConnection(final EventLoop callerEventLoop) {
    // Make a new H2 connection to the target backend
    final Timer.Sample connectSample = Timer.start();

    return new Bootstrap()
        // If the caller is already on one of our event loops, connect on that loop so this connection runs on the
        // caller's thread
        .group(eventLoops.contains(callerEventLoop) ? callerEventLoop : eventLoopGroup)
        .channel(channelClass)
        .handler(new ChannelInitializer<>() {
          @Override
          protected void initChannel(final Channel ch) {
            ch.pipeline()
                .addLast(Http2FrameCodecBuilder.forClient().initialSettings(Http2Settings.defaultSettings()).build());
            // Http2MultiplexHandler takes handler that is added to new inbound streams. A client Http2MultiplexHandler
            // like we're defining here should never receive an inbound H2 stream so we can just pass a noop handler
            ch.pipeline().addLast(new Http2MultiplexHandler(new NoopInboundStreamHandler()));
            ch.pipeline().addLast(BACKEND_CONNECTION_EXCEPTION_HANDLER);
          }
        })
        .connect(targetAddress)
        .addListener((ChannelFuture connectFuture) ->
          connectSample.stop(Timer.builder(BACKEND_CONNECT_TIMER_NAME)
              .tag(BACKEND_TAG, targetAddress.toString())
              .tag("outcome", connectFuture.isSuccess() ? "success" : "failure")
              .register(Metrics.globalRegistry)));
  }

  @Override
  public SocketAddress address() {
    return targetAddress;
  }

  private static class NoopInboundStreamHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) {
      logger.error("Inbound stream handler was registered when no inbound streams expected");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      logger.error("Received unexpected message: {} on inbound stream from backend", msg);
      super.channelRead(ctx, msg);
    }
  }
}
