/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.Metrics;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.SimpleUserEventChannelHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SniHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Mapping;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

/// An HTTP/2 server that proxies H2 streams to configurable backends via path-based routing
public class OmnibusH2Server implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(OmnibusH2Server.class);

  private static final OmnibusConnectionCounterHandler CONNECT_COUNTER = new OmnibusConnectionCounterHandler();
  private static final OmnibusExceptionHandler HANDSHAKE_EXCEPTION_HANDLER =
      new OmnibusExceptionHandler("omnibus-handshake", List.of(SocketException.class, DecoderException.class, IOException.class));
  private static final OmnibusExceptionHandler SESSION_EXCEPTION_HANDLER =
      new OmnibusExceptionHandler("omnibus-session", List.of(Http2Exception.class));
  private static final String IDLE_DISCONNECT_COUNTER_NAME = MetricsUtil.name(OmnibusH2Server.class, "idleDisconnect");

  private final @Nullable Mapping<String, SslContext> sslContextBySni;
  private final OmnibusRouter router;
  private final Duration idleTimeout;
  private final DefaultEventLoopGroup localEventLoopGroup;
  private final NioEventLoopGroup nioEventLoopGroup;
  private final SocketAddress bindAddress;

  private Channel serverChannel;

  /// Create an omnibus server
  ///
  /// @param sslContextBySni     If not null, a mapping between domain (SNI) and the appropriate SslContext to use for
  ///                            that SNI. If null, the server will not include TLS (h2c with prior-knowledge)
  /// @param nioEventLoopGroup   Event loop to use for all NIO channel pipelines
  /// @param localEventLoopGroup Event loop to use for all local channel pipelines
  /// @param bindAddress         The address the server should listen on
  /// @param router              How the server should select backends based on request paths
  public OmnibusH2Server(
      final @Nullable Mapping<String, SslContext> sslContextBySni,
      final NioEventLoopGroup nioEventLoopGroup,
      final DefaultEventLoopGroup localEventLoopGroup,
      final SocketAddress bindAddress,
      final OmnibusRouter router,
      final Duration idleTimeout) {
    this.sslContextBySni = sslContextBySni;
    this.nioEventLoopGroup = nioEventLoopGroup;
    this.localEventLoopGroup = localEventLoopGroup;
    this.bindAddress = bindAddress;
    this.router = router;
    this.idleTimeout = idleTimeout;
  }

  @Override
  public void start() throws Exception {
    if (this.sslContextBySni == null) {
      logger.warn("No SSL configuration provided for OmnibusH2Server, serving h2c");
    }

    if (ByteBufAllocator.DEFAULT instanceof ByteBufAllocatorMetricProvider alloc) {
      Metrics.gauge(MetricsUtil.name(OmnibusH2Server.class, "nettyUsedDirectMemory"),
          alloc,
          allocator -> allocator.metric().usedDirectMemory());
      Metrics.gauge(MetricsUtil.name(OmnibusH2Server.class, "nettyUsedHeapMemory"),
          alloc,
          allocator -> allocator.metric().usedHeapMemory());
    }

    final ServerBootstrap bootstrap = new ServerBootstrap()
        .group(nioEventLoopGroup)
        .channel(NioServerSocketChannel.class)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(final SocketChannel ch) {
            ch.pipeline().addLast(new IdleStateHandler(0, 0, idleTimeout.toMillis(), TimeUnit.MILLISECONDS));
            ch.pipeline().addLast(new SimpleUserEventChannelHandler<IdleStateEvent>() {
              @Override
              protected void eventReceived(final ChannelHandlerContext ctx, final IdleStateEvent evt) {
                Metrics.counter(IDLE_DISCONNECT_COUNTER_NAME, "type", evt.state().name()).increment();
                ctx.close();
              }
            });
            ch.pipeline().addLast(CONNECT_COUNTER);
            ch.pipeline().addLast(new ProxyProtocolHandler());
            ch.pipeline().addLast(new ProxyMessageAttributeSetterHandler());
            if (sslContextBySni == null) {
              configureH2Pipeline(ch.pipeline());
            } else {
              ch.pipeline().addLast(new SniHandler(sslContextBySni));
              ch.pipeline().addLast(new ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_2) {
                @Override
                protected void configurePipeline(final ChannelHandlerContext ctx, final String protocol) {
                  if (!ApplicationProtocolNames.HTTP_2.equals(protocol)) {
                    // HTTP/2 should be enforced by our ALPN settings
                    logger.error("Unsupported protocol negotiated: {}, closing connection", protocol);
                    ctx.close();
                    return;
                  }
                  configureH2Pipeline(ctx.pipeline());
                }
              });
              ch.pipeline().addLast(HANDSHAKE_EXCEPTION_HANDLER);
            }
          }
        });

    serverChannel = bootstrap.bind(bindAddress).sync().channel();
    logger.info("Omnibus server listening on {}", getLocalAddress());
  }

  @VisibleForTesting
  InetSocketAddress getLocalAddress() {
    return (InetSocketAddress) serverChannel.localAddress();
  }

  @Override
  public void stop() {
    if (serverChannel != null) {
      logger.info("Stopping omnibus server");
      serverChannel.close().syncUninterruptibly();
      logger.info("Omnibus server stopped");
    }
  }

  private void configureH2Pipeline(final ChannelPipeline pipeline) {
    // Advertise support for RFC-8441 extended connect
    final Http2Settings settings = Http2Settings.defaultSettings().connectProtocolEnabled(true);
    pipeline.addLast(Http2FrameCodecBuilder.forServer().initialSettings(settings).build());
    pipeline.addLast(new Http2MultiplexHandler(new ChannelInitializer<Http2StreamChannel>() {
      @Override
      protected void initChannel(final Http2StreamChannel ch) {
        ch.pipeline().addLast(new OmnibusH2StreamHandler(nioEventLoopGroup, localEventLoopGroup, router));
      }
    }));
    pipeline.addLast(SESSION_EXCEPTION_HANDLER);
  }
}
