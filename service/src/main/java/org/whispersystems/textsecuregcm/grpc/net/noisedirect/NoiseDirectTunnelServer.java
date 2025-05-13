package org.whispersystems.textsecuregcm.grpc.net.noisedirect;

import com.google.common.annotations.VisibleForTesting;
import com.southernstorm.noise.protocol.Noise;
import io.dropwizard.lifecycle.Managed;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.net.InetSocketAddress;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.grpc.net.ErrorHandler;
import org.whispersystems.textsecuregcm.grpc.net.EstablishLocalGrpcConnectionHandler;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;
import org.whispersystems.textsecuregcm.grpc.net.HAProxyMessageHandler;
import org.whispersystems.textsecuregcm.grpc.net.NoiseHandshakeHandler;
import org.whispersystems.textsecuregcm.grpc.net.ProxyProtocolDetectionHandler;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;

/**
 * A NoiseDirectTunnelServer accepts traffic from the public internet (in the form of Noise packets framed by a custom
 * binary framing protocol) and passes it through to a local gRPC server.
 */
public class NoiseDirectTunnelServer implements Managed {

  private final ServerBootstrap bootstrap;
  private ServerSocketChannel channel;

  private static final Logger log = LoggerFactory.getLogger(NoiseDirectTunnelServer.class);

  public NoiseDirectTunnelServer(final int port,
      final NioEventLoopGroup eventLoopGroup,
      final GrpcClientConnectionManager grpcClientConnectionManager,
      final ClientPublicKeysManager clientPublicKeysManager,
      final ECKeyPair ecKeyPair,
      final LocalAddress authenticatedGrpcServerAddress,
      final LocalAddress anonymousGrpcServerAddress) {

    this.bootstrap = new ServerBootstrap()
        .group(eventLoopGroup)
        .channel(NioServerSocketChannel.class)
        .localAddress(port)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel socketChannel) {
            socketChannel.pipeline()
                .addLast(new ProxyProtocolDetectionHandler())
                .addLast(new HAProxyMessageHandler())
                // frame byte followed by a 2-byte length field
                .addLast(new LengthFieldBasedFrameDecoder(Noise.MAX_PACKET_LEN, 1, 2))
                // Parses NoiseDirectFrames from wire bytes and vice versa
                .addLast(new NoiseDirectFrameCodec())
                // Terminate the connection if the client sends us a close frame
                .addLast(new NoiseDirectInboundCloseHandler())
                // Turn generic OutboundCloseErrorMessages into noise direct error frames
                .addLast(new NoiseDirectOutboundErrorHandler())
                // Forwards the first payload supplemented with handshake metadata, and then replaces itself with a
                // NoiseDirectDataFrameCodec to handle subsequent data frames
                .addLast(new NoiseDirectHandshakeSelector())
                // Performs the noise handshake and then replace itself with a NoiseHandler
                .addLast(new NoiseHandshakeHandler(clientPublicKeysManager, ecKeyPair))
                // This handler will open a local connection to the appropriate gRPC server and install a ProxyHandler
                // once the Noise handshake has completed
                .addLast(new EstablishLocalGrpcConnectionHandler(
                    grpcClientConnectionManager, authenticatedGrpcServerAddress, anonymousGrpcServerAddress))
                .addLast(new ErrorHandler());
          }
        });
  }

  @VisibleForTesting
  public InetSocketAddress getLocalAddress() {
    return channel.localAddress();
  }

  @Override
  public void start() throws InterruptedException {
    channel = (ServerSocketChannel) bootstrap.bind().await().channel();
  }

  @Override
  public void stop() throws InterruptedException {
    if (channel != null) {
      channel.close().await();
    }
  }
}
