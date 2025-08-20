package org.whispersystems.textsecuregcm.grpc.net;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.util.ReferenceCountUtil;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;

/**
 * An "establish local connection" handler waits for a Noise handshake to complete upstream in the pipeline, buffering
 * any inbound messages until the connection is fully-established, and then opens a proxy connection to a local gRPC
 * server.
 */
public class EstablishLocalGrpcConnectionHandler extends ChannelInboundHandlerAdapter {
  private static final Logger log = LoggerFactory.getLogger(EstablishLocalGrpcConnectionHandler.class);

  private final GrpcClientConnectionManager grpcClientConnectionManager;

  private final LocalAddress authenticatedGrpcServerAddress;
  private final LocalAddress anonymousGrpcServerAddress;
  private final FramingType framingType;

  private final List<Object> pendingReads = new ArrayList<>();

  private static final String CONNECTION_ESTABLISHED_COUNTER_NAME = MetricsUtil.name(EstablishLocalGrpcConnectionHandler.class, "established");

  public EstablishLocalGrpcConnectionHandler(final GrpcClientConnectionManager grpcClientConnectionManager,
      final LocalAddress authenticatedGrpcServerAddress,
      final LocalAddress anonymousGrpcServerAddress,
      final FramingType framingType) {

    this.grpcClientConnectionManager = grpcClientConnectionManager;

    this.authenticatedGrpcServerAddress = authenticatedGrpcServerAddress;
    this.anonymousGrpcServerAddress = anonymousGrpcServerAddress;
    this.framingType = framingType;
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) {
    pendingReads.add(message);
  }

  @Override
  public void userEventTriggered(final ChannelHandlerContext remoteChannelContext, final Object event) {
    if (event instanceof NoiseIdentityDeterminedEvent(
        final Optional<AuthenticatedDevice> authenticatedDevice,
        InetAddress remoteAddress, String userAgent, String acceptLanguage)) {
      // We assume that we'll only get a completed handshake event if the handshake met all authentication requirements
      // for the requested service. If the handshake doesn't have an authenticated device, we assume we're trying to
      // connect to the anonymous service. If it does have an authenticated device, we assume we're aiming for the
      // authenticated service.
      final LocalAddress grpcServerAddress = authenticatedDevice.isPresent()
          ? authenticatedGrpcServerAddress
          : anonymousGrpcServerAddress;

      GrpcClientConnectionManager.handleHandshakeInitiated(
          remoteChannelContext.channel(), remoteAddress, userAgent, acceptLanguage);

      final List<Tag> tags = UserAgentTagUtil.getLibsignalAndPlatformTags(userAgent);
      Metrics.counter(CONNECTION_ESTABLISHED_COUNTER_NAME, Tags.of(tags)
          .and("authenticated", Boolean.toString(authenticatedDevice.isPresent()))
          .and("framingType", framingType.name()))
          .increment();

      new Bootstrap()
          .remoteAddress(grpcServerAddress)
          .channel(LocalChannel.class)
          .group(remoteChannelContext.channel().eventLoop())
          .handler(new ChannelInitializer<LocalChannel>() {
            @Override
            protected void initChannel(final LocalChannel localChannel) {
              localChannel.pipeline().addLast(new ProxyHandler(remoteChannelContext.channel()));
            }
          })
          .connect()
          .addListener((ChannelFutureListener) localChannelFuture -> {
            if (localChannelFuture.isSuccess()) {
              grpcClientConnectionManager.handleConnectionEstablished((LocalChannel) localChannelFuture.channel(),
                  remoteChannelContext.channel(),
                  authenticatedDevice);

              // Close the local connection if the remote channel closes and vice versa
              remoteChannelContext.channel().closeFuture().addListener(closeFuture -> localChannelFuture.channel().close());
              localChannelFuture.channel().closeFuture().addListener(closeFuture ->
                  remoteChannelContext.channel()
                      .write(new OutboundCloseErrorMessage(OutboundCloseErrorMessage.Code.SERVER_CLOSED, "server closed"))
                      .addListener(ChannelFutureListener.CLOSE_ON_FAILURE));

              remoteChannelContext.pipeline()
                  .addAfter(remoteChannelContext.name(), null, new ProxyHandler(localChannelFuture.channel()));

              // Flush any buffered reads we accumulated while waiting to open the connection
              pendingReads.forEach(remoteChannelContext::fireChannelRead);
              pendingReads.clear();

              remoteChannelContext.pipeline().remove(EstablishLocalGrpcConnectionHandler.this);
            } else {
              log.warn("Failed to establish local connection to gRPC server", localChannelFuture.cause());
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
