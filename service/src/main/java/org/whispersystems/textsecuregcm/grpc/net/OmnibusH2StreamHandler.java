/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.DefaultHttp2ResetFrame;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Handler added on each newly created [Http2StreamChannel] on an H2 connection. Inspects the [Http2HeadersFrame] and
/// determines which backend to forward the stream to, and then proxies frames to and from the backend.
///
/// When this handler receives an H2 header for a new stream-id=X on our parent H2 connection it will
/// - Receive the stream-id=X header and check the path to determine the correct backend
/// - Make a new H2 connection to the backend
/// - Forward the header with stream-id=Y to the backend
/// - Install a [H2FrameProxyHandler] on the backend stream pipeline that forwards the received stream-id=Y frames from
///   the backend back to the client on stream-id=X
/// - Install a [H2FrameProxyHandler] on the client stream pipeline forwards the received stream-id=X frames from the
///   client to the backend on stream-id=Y
public class OmnibusH2StreamHandler extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(OmnibusH2StreamHandler.class);

  private static final OmnibusExceptionHandler BACKEND_CONNECTION_EXCEPTION_HANDLER =
      new OmnibusExceptionHandler("backend-connection", List.of());
  private static final String BACKEND_STREAM_COUNTER_NAME = name(OmnibusH2StreamHandler.class, "backendStream");
  private static final String BACKEND_CONNECT_DURATION_NAME = name(OmnibusH2StreamHandler.class,
      "backendConnectDuration");
  private static final String BACKEND_TAG = "backend";

  private final OmnibusRouter router;

  private final DefaultEventLoopGroup localEventLoopGroup;
  private final NioEventLoopGroup nioEventLoopGroup;

  public OmnibusH2StreamHandler(
      final NioEventLoopGroup nioEventLoopGroup,
      final DefaultEventLoopGroup localEventLoopGroup,
      final OmnibusRouter router) {
    this.router = router;
    this.localEventLoopGroup = localEventLoopGroup;
    this.nioEventLoopGroup = nioEventLoopGroup;
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (!(msg instanceof Http2HeadersFrame headersFrame)) {
      logger.warn("Expected initial HEADERS frame but got {}", msg.getClass().getSimpleName());
      ReferenceCountUtil.release(msg);
      ctx.close();
      return;
    }

    // We don't expect headers frames to come with manually managed memory attached. Assert this in case this changes
    // in the future, but for now we don't have to worry about freeing the headers frame
    assert !(headersFrame instanceof ReferenceCounted);

    // Disable reading from the client because we want to wait until we make the backend connection and install the
    // forwarding handler before processing any more frames.
    ctx.channel().config().setAutoRead(false);

    // Select the target backend based on the path
    final String path = Optional.ofNullable(headersFrame.headers().path()).map(CharSequence::toString).orElse("");
    final SocketAddress target = router.match(path);
    final String backendTag = target.toString();

    Metrics.counter(BACKEND_STREAM_COUNTER_NAME, BACKEND_TAG, backendTag).increment();

    // Set X-Forwarded-For from the PROXY protocol header if present, otherwise via the remote address
    final InetAddress proxyRemoteAddress = ctx.channel().parent()
        .attr(ProxyMessageAttributeSetterHandler.PROXY_REMOTE_ADDRESS)
        .get();
    headersFrame.headers().set("x-forwarded-for", proxyRemoteAddress != null
        ? proxyRemoteAddress.getHostAddress()
        : ((InetSocketAddress) ctx.channel().remoteAddress()).getHostString());

    // Make a new H2 connection to the target backend
    final Timer.Sample connectSample = Timer.start();
    new Bootstrap()
        .group(selectEventLoop(ctx, target))
        .channel(target instanceof LocalAddress ? LocalChannel.class : NioSocketChannel.class)
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
        .connect(target)
        .addListener((ChannelFuture connectFuture) -> {
          connectSample.stop(Timer.builder(BACKEND_CONNECT_DURATION_NAME)
              .tag(BACKEND_TAG, backendTag)
              .tag("outcome", connectFuture.isSuccess() ? "success" : "failure")
              .register(Metrics.globalRegistry));

          if (!connectFuture.isSuccess()) {
            // Close the client stream with a 502: Bad Gateway if the backend wasn't available
            logger.warn("Failed to connect to backend {}", target, connectFuture.cause());
            ctx.channel()
                .writeAndFlush(new DefaultHttp2HeadersFrame(
                    new DefaultHttp2Headers().status("502"), true))
                .addListener(ChannelFutureListener.CLOSE);
            return;
          }

          // Connected, open a new H2 stream to the backend so we can proxy the client's frames
          logger.trace("Opening a HTTP/2 stream to the backend {}", target);
          final Channel backendConnection = connectFuture.channel();
          createBackendProxyStream(ctx, backendConnection, headersFrame);
        });
  }

  /// Create a proxy stream on the provided `backendConnection` that forwards H2 frames to/from the client H2 stream.
  ///
  /// @param clientStreamCtx   The context for a client H2 stream that targets the backend
  /// @param backendConnection An established H2 connection [Channel], on which a new h2 stream will be opened
  /// @param headersFrame      The first `headersFrame` from the client h2 stream that should be forwarded to the new
  ///                          backend stream
  private void createBackendProxyStream(
      final ChannelHandlerContext clientStreamCtx,
      final Channel backendConnection,
      final Http2HeadersFrame headersFrame) {
    new Http2StreamChannelBootstrap(backendConnection)
        // Forwards response frames from the backend back to the client stream
        .handler(new H2FrameProxyHandler(clientStreamCtx.channel(), "responseStream"))
        .open()
        .addListener((io.netty.util.concurrent.Future<Http2StreamChannel> streamFuture) -> {
          if (!streamFuture.isSuccess()) {
            logger.warn("Failed to open backend stream", streamFuture.cause());
            clientStreamCtx.channel()
                .writeAndFlush(new DefaultHttp2ResetFrame(Http2Error.INTERNAL_ERROR))
                .addListener(ChannelFutureListener.CLOSE);
            backendConnection.close();
            return;
          }

          final Http2StreamChannel backendStream = streamFuture.getNow();

          // Close the entire H2 connection whenever the stream we just opened closes. We only plan on using
          // a single stream on this connection.
          backendStream.closeFuture().addListener(_ -> backendConnection.close());

          // We're going to modify the inbound H2 stream channel, which runs on a different eventloop than the
          // outbound channel we've made to the backend. We have to submit our updates back to the inbound
          // channel's event loop for thread safety
          clientStreamCtx.channel().eventLoop().execute(() -> {

            if (!clientStreamCtx.channel().isActive()) {
              // The client disconnected already and the client pipeline is already torn down.
              backendConnection.close();
              return;
            }

            // Install proxy on client stream, remove this handler, then fire the buffered headers through the proxy
            clientStreamCtx.pipeline().replace(
                OmnibusH2StreamHandler.this,
                "backend-to-client-proxy",
                new H2FrameProxyHandler(backendStream, "requestStream"));
            clientStreamCtx.channel().pipeline().fireChannelRead(headersFrame);

            // Resume inbound reads, which should now be forwarded
            clientStreamCtx.channel().config().setAutoRead(true);
          });
        });
  }

  private EventLoopGroup selectEventLoop(final ChannelHandlerContext inboundCtx, SocketAddress target) {
    final boolean localInbound = inboundCtx.channel() instanceof LocalChannel;
    final boolean localTarget = target instanceof LocalAddress;

    // If the inbound eventloop matches the target type, we can just reuse the inbound's event loop
    if (localInbound == localTarget) {
      return inboundCtx.channel().eventLoop();
    }

    return localTarget ? this.localEventLoopGroup : this.nioEventLoopGroup;
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
