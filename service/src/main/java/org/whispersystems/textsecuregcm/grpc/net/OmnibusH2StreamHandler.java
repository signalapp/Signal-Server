/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.DefaultHttp2ResetFrame;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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

  private static final String BACKEND_STREAM_COUNTER_NAME = name(OmnibusH2StreamHandler.class, "backendStream");
  private static final String BACKEND_TAG = "backend";

  private final OmnibusRouter router;

  public OmnibusH2StreamHandler(final OmnibusRouter router) {
    this.router = router;
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
    final H2ConnectionFactory connectionFactory = router.match(path);
    final String backendTag = connectionFactory.address().toString();

    Metrics.counter(BACKEND_STREAM_COUNTER_NAME, BACKEND_TAG, backendTag).increment();

    // Set X-Forwarded-For from the PROXY protocol header if present, otherwise via the remote address
    final InetAddress proxyRemoteAddress = ctx.channel().parent()
        .attr(ProxyMessageAttributeSetterHandler.PROXY_REMOTE_ADDRESS)
        .get();
    headersFrame.headers().set("x-forwarded-for", proxyRemoteAddress != null
        ? proxyRemoteAddress.getHostAddress()
        : ((InetSocketAddress) ctx.channel().remoteAddress()).getHostString());

    // Make an H2 connection to the target backend
    connectionFactory.getConnection(ctx.channel().eventLoop())
        .addListener((ChannelFuture connectFuture) -> {
          if (!connectFuture.isSuccess()) {
            // Close the client stream with a 502: Bad Gateway if the backend wasn't available
            logger.warn("Failed to connect to backend {}", backendTag, connectFuture.cause());
            ctx.channel()
                .writeAndFlush(new DefaultHttp2HeadersFrame(
                    new DefaultHttp2Headers().status("502"), true))
                .addListener(ChannelFutureListener.CLOSE);
            return;
          }

          // Connected, open a new H2 stream to the backend so we can proxy the client's frames
          logger.trace("Opening a HTTP/2 stream to the backend {}", backendTag);
          final Channel backendConnection = connectFuture.channel();
          createBackendProxyStream(ctx, backendConnection, headersFrame, backendTag);
        });
  }

  /// Create a proxy stream on the provided `backendConnection` that forwards H2 frames to/from the client H2 stream.
  ///
  /// @param clientStreamCtx   The context for a client H2 stream that targets the backend
  /// @param backendConnection An established H2 connection [Channel], on which a new h2 stream will be opened
  /// @param headersFrame      The first `headersFrame` from the client h2 stream that should be forwarded to the new
  ///                          backend stream
  /// @param backendTag        The backend this stream targets, for metrics
  private void createBackendProxyStream(
      final ChannelHandlerContext clientStreamCtx,
      final Channel backendConnection,
      final Http2HeadersFrame headersFrame,
      final String backendTag) {
    new Http2StreamChannelBootstrap(backendConnection)
        // Forwards response frames from the backend back to the client stream
        .handler(new H2FrameProxyHandler(clientStreamCtx.channel(), "responseStream", backendTag))
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
                new H2FrameProxyHandler(backendStream, "requestStream", backendTag));
            clientStreamCtx.channel().pipeline().fireChannelRead(headersFrame);

            // Resume inbound reads, which should now be forwarded
            clientStreamCtx.channel().config().setAutoRead(true);
          });
        });
  }
}
