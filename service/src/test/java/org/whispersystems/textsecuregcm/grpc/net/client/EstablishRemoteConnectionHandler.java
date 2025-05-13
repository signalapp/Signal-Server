package org.whispersystems.textsecuregcm.grpc.net.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.whispersystems.textsecuregcm.grpc.net.NoiseTunnelProtos;
import org.whispersystems.textsecuregcm.grpc.net.ProxyHandler;

/**
 * Handler that takes plaintext inbound messages from a gRPC client and forwards them over the noise tunnel to a remote
 * gRPC server.
 * <p>
 * This handler waits until the first gRPC client message is ready and then establishes a connection with the remote
 * gRPC server. It expects the provided remoteHandlerStack to emit a {@link ReadyForNoiseHandshakeEvent} when the remote
 * connection is ready for its first inbound payload, and to emit a {@link NoiseClientHandshakeCompleteEvent} when the
 * handshake is finished.
 */
class EstablishRemoteConnectionHandler extends ChannelInboundHandlerAdapter {

  private final List<ChannelHandler> remoteHandlerStack;
  private final NoiseTunnelProtos.HandshakeInit handshakeInit;

  private final SocketAddress remoteServerAddress;
  // If provided, will be sent with the payload in the noise handshake

  private final List<Object> pendingReads = new ArrayList<>();

  private static final String NOISE_HANDSHAKE_HANDLER_NAME = "noise-handshake";

  EstablishRemoteConnectionHandler(
      final List<ChannelHandler> remoteHandlerStack,
      final SocketAddress remoteServerAddress,
      final NoiseTunnelProtos.HandshakeInit handshakeInit) {
    this.remoteHandlerStack = remoteHandlerStack;
    this.handshakeInit = handshakeInit;
    this.remoteServerAddress = remoteServerAddress;
  }

  @Override
  public void handlerAdded(final ChannelHandlerContext localContext) {
    new Bootstrap()
        .channel(NioSocketChannel.class)
        .group(localContext.channel().eventLoop())
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(final SocketChannel channel) throws Exception {

            for (ChannelHandler handler : remoteHandlerStack) {
              channel.pipeline().addLast(handler);
            }
            channel.pipeline()
                .addLast(NOISE_HANDSHAKE_HANDLER_NAME, new ChannelInboundHandlerAdapter() {
                  @Override
                  public void userEventTriggered(final ChannelHandlerContext remoteContext, final Object event)
                      throws Exception {
                    switch (event) {
                      case ReadyForNoiseHandshakeEvent ignored ->
                          remoteContext.writeAndFlush(Unpooled.wrappedBuffer(handshakeInit.toByteArray()))
                              .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
                      case NoiseClientHandshakeCompleteEvent(NoiseTunnelProtos.HandshakeResponse handshakeResponse) -> {
                        remoteContext.pipeline()
                            .replace(NOISE_HANDSHAKE_HANDLER_NAME, null, new ProxyHandler(localContext.channel()));
                        localContext.pipeline().addLast(new ProxyHandler(remoteContext.channel()));

                        // If there was a payload response on the handshake, write it back to our gRPC client
                        if (!handshakeResponse.getFastOpenResponse().isEmpty()) {
                          localContext.writeAndFlush(Unpooled.wrappedBuffer(handshakeResponse
                              .getFastOpenResponse()
                              .asReadOnlyByteBuffer()));
                        }

                        // Forward any messages we got from our gRPC client, now will be proxied to the remote context
                        pendingReads.forEach(localContext::fireChannelRead);
                        pendingReads.clear();
                        localContext.pipeline().remove(EstablishRemoteConnectionHandler.this);
                      }
                      default -> {
                      }
                    }
                    super.userEventTriggered(remoteContext, event);
                  }
                })
                .addLast(new ClientErrorHandler());
          }
        })
        .connect(remoteServerAddress)
        .addListener((ChannelFutureListener) future -> {
          if (future.isSuccess()) {
            // Close the local connection if the remote channel closes and vice versa
            future.channel().closeFuture().addListener(closeFuture -> localContext.channel().close());
            localContext.channel().closeFuture().addListener(closeFuture -> future.channel().close());
          } else {
            localContext.close();
          }
        });
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) {
    pendingReads.add(message);
  }

  @Override
  public void handlerRemoved(final ChannelHandlerContext context) {
    pendingReads.forEach(ReferenceCountUtil::release);
    pendingReads.clear();
  }

}
