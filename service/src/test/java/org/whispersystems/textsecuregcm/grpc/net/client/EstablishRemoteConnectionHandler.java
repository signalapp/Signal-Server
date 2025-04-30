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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
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
  @Nullable
  private final AuthenticatedDevice authenticatedDevice;

  private final SocketAddress remoteServerAddress;
  // If provided, will be sent with the payload in the noise handshake
  private final byte[] fastOpenRequest;

  private final List<Object> pendingReads = new ArrayList<>();

  private static final String NOISE_HANDSHAKE_HANDLER_NAME = "noise-handshake";

  EstablishRemoteConnectionHandler(
      final List<ChannelHandler> remoteHandlerStack,
      @Nullable final AuthenticatedDevice authenticatedDevice,
      final SocketAddress remoteServerAddress,
      @Nullable byte[] fastOpenRequest) {
    this.remoteHandlerStack = remoteHandlerStack;
    this.authenticatedDevice = authenticatedDevice;
    this.remoteServerAddress = remoteServerAddress;
    this.fastOpenRequest = fastOpenRequest == null ? new byte[0] : fastOpenRequest;
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
                          remoteContext.writeAndFlush(Unpooled.wrappedBuffer(initialPayload()))
                              .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
                      case NoiseClientHandshakeCompleteEvent(Optional<byte[]> fastResponse) -> {
                        remoteContext.pipeline()
                            .replace(NOISE_HANDSHAKE_HANDLER_NAME, null, new ProxyHandler(localContext.channel()));
                        localContext.pipeline().addLast(new ProxyHandler(remoteContext.channel()));

                        // If there was a payload response on the handshake, write it back to our gRPC client
                        fastResponse.ifPresent(plaintext ->
                            localContext.writeAndFlush(Unpooled.wrappedBuffer(plaintext)));

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

  private byte[] initialPayload() {
    if (authenticatedDevice == null) {
      return fastOpenRequest;
    }

    final ByteBuffer bb = ByteBuffer.allocate(17 + fastOpenRequest.length);
    bb.putLong(authenticatedDevice.accountIdentifier().getMostSignificantBits());
    bb.putLong(authenticatedDevice.accountIdentifier().getLeastSignificantBits());
    bb.put(authenticatedDevice.deviceId());
    bb.put(fastOpenRequest);
    bb.flip();
    return bb.array();
  }
}
