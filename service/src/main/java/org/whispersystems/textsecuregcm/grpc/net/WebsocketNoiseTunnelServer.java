package org.whispersystems.textsecuregcm.grpc.net;

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
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProtocols;
import io.netty.handler.ssl.SslProvider;
import java.net.InetSocketAddress;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.concurrent.Executor;
import javax.net.ssl.SSLException;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;

/**
 * A WebSocket/Noise tunnel server accepts traffic from the public internet (in the form of Noise packets framed by
 * binary WebSocket frames) and passes it through to a local gRPC server.
 */
public class WebsocketNoiseTunnelServer implements Managed {

  private final ServerBootstrap bootstrap;
  private ServerSocketChannel channel;

  static final String AUTHENTICATED_SERVICE_PATH = "/authenticated";
  static final String ANONYMOUS_SERVICE_PATH = "/anonymous";

  private static final Logger log = LoggerFactory.getLogger(WebsocketNoiseTunnelServer.class);

  public WebsocketNoiseTunnelServer(final int websocketPort,
      final X509Certificate[] tlsCertificateChain,
      final PrivateKey tlsPrivateKey,
      final NioEventLoopGroup eventLoopGroup,
      final Executor delegatedTaskExecutor,
      final ClientPublicKeysManager clientPublicKeysManager,
      final ECKeyPair ecKeyPair,
      final byte[] publicKeySignature,
      final LocalAddress authenticatedGrpcServerAddress,
      final LocalAddress anonymousGrpcServerAddress) throws SSLException {

    final SslProvider sslProvider;

    if (OpenSsl.isAvailable()) {
      log.info("Native OpenSSL provider is available; will use native provider");
      sslProvider = SslProvider.OPENSSL;
    } else {
      log.info("No native SSL provider available; will use JDK provider");
      sslProvider = SslProvider.JDK;
    }

    final SslContext sslContext = SslContextBuilder.forServer(tlsPrivateKey, tlsCertificateChain)
        .clientAuth(ClientAuth.NONE)
        .protocols(SslProtocols.TLS_v1_3)
        .sslProvider(sslProvider)
        .build();

    this.bootstrap = new ServerBootstrap()
        .group(eventLoopGroup)
        .channel(NioServerSocketChannel.class)
        .localAddress(websocketPort)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel socketChannel) {
            socketChannel.pipeline()
                .addLast(sslContext.newHandler(socketChannel.alloc(), delegatedTaskExecutor))
                .addLast(new HttpServerCodec())
                .addLast(new HttpObjectAggregator(Noise.MAX_PACKET_LEN))
                // The WebSocket opening handshake handler will remove itself from the pipeline once it has received a valid WebSocket upgrade
                // request and passed it down the pipeline
                .addLast(new WebSocketOpeningHandshakeHandler(AUTHENTICATED_SERVICE_PATH, ANONYMOUS_SERVICE_PATH))
                .addLast(new WebSocketServerProtocolHandler("/", true))
                .addLast(new RejectUnsupportedMessagesHandler())
                // The WebSocket handshake complete listener will replace itself with an appropriate Noise handshake handler once
                // a WebSocket handshake has been completed
                .addLast(new WebsocketHandshakeCompleteListener(clientPublicKeysManager, ecKeyPair, publicKeySignature))
                // This handler will open a local connection to the appropriate gRPC server and install a ProxyHandler
                // once the Noise handshake has completed
                .addLast(new EstablishLocalGrpcConnectionHandler(authenticatedGrpcServerAddress, anonymousGrpcServerAddress))
                .addLast(new ErrorHandler());
          }
        });
  }

  @VisibleForTesting
  InetSocketAddress getLocalAddress() {
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
