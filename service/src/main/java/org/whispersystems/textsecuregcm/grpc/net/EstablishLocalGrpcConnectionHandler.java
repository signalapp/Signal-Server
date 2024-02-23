package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An "establish local connection" handler waits for a Noise handshake to complete upstream in the pipeline, buffering
 * any inbound messages until the connection is fully-established, and then opens a proxy connection to a local gRPC
 * server.
 */
class EstablishLocalGrpcConnectionHandler extends ChannelInboundHandlerAdapter {

  private final LocalAddress authenticatedGrpcServerAddress;
  private final LocalAddress anonymousGrpcServerAddress;
  private final List<Object> pendingReads = new ArrayList<>();

  private static final Logger log = LoggerFactory.getLogger(EstablishLocalGrpcConnectionHandler.class);

  public EstablishLocalGrpcConnectionHandler(final LocalAddress authenticatedGrpcServerAddress,
      final LocalAddress anonymousGrpcServerAddress) {

    this.authenticatedGrpcServerAddress = authenticatedGrpcServerAddress;
    this.anonymousGrpcServerAddress = anonymousGrpcServerAddress;
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) {
    pendingReads.add(message);
  }

  @Override
  public void userEventTriggered(final ChannelHandlerContext remoteChannelContext, final Object event) throws Exception {
    if (event instanceof NoiseHandshakeCompleteEvent noiseHandshakeCompleteEvent) {
      // We assume that we'll only get a completed handshake event if the handshake met all authentication requirements
      // for the requested service. If the handshake doesn't have an authenticated device, we assume we're trying to
      // connect to the anonymous service. If it does have an authenticated device, we assume we're aiming for the
      // authenticated service.
      final LocalAddress grpcServerAddress = noiseHandshakeCompleteEvent.authenticatedDevice().isPresent()
          ? authenticatedGrpcServerAddress
          : anonymousGrpcServerAddress;

      new Bootstrap()
          .remoteAddress(grpcServerAddress)
          // TODO Set local address
          .channel(LocalChannel.class)
          .group(remoteChannelContext.channel().eventLoop())
          .handler(new ChannelInitializer<LocalChannel>() {
            @Override
            protected void initChannel(final LocalChannel localChannel) {
              localChannel.pipeline().addLast(new ProxyHandler(remoteChannelContext.channel()));
            }
          })
          .connect()
          .addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
              // Close the local connection if the remote channel closes and vice versa
              remoteChannelContext.channel().closeFuture().addListener(closeFuture -> future.channel().close());
              future.channel().closeFuture().addListener(closeFuture ->
                  remoteChannelContext.write(new CloseWebSocketFrame(WebSocketCloseStatus.SERVICE_RESTART)));

              remoteChannelContext.pipeline()
                  .addAfter(remoteChannelContext.name(), null, new ProxyHandler(future.channel()));

              // Flush any buffered reads we accumulated while waiting to open the connection
              pendingReads.forEach(remoteChannelContext::fireChannelRead);
              pendingReads.clear();

              remoteChannelContext.pipeline().remove(EstablishLocalGrpcConnectionHandler.this);
            } else {
              log.warn("Failed to establish local connection to gRPC server", future.cause());
              remoteChannelContext.close();
            }
          });
    }

    remoteChannelContext.fireUserEventTriggered(event);
  }

  @Override
  public void handlerRemoved(final ChannelHandlerContext context) {
    pendingReads.forEach(ReferenceCountUtil::release);
    pendingReads.clear();
  }
}
