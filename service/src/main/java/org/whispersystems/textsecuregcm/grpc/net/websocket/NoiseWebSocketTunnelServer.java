package org.whispersystems.textsecuregcm.grpc.net.websocket;

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
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.grpc.net.*;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;

/**
 * A Noise-over-WebSocket tunnel server accepts traffic from the public internet (in the form of Noise packets framed by
 * binary WebSocket frames) and passes it through to a local gRPC server.
 */
public class NoiseWebSocketTunnelServer implements Managed {

  private final ServerBootstrap bootstrap;
  private ServerSocketChannel channel;

  static final String AUTHENTICATED_SERVICE_PATH = "/authenticated";
  static final String ANONYMOUS_SERVICE_PATH = "/anonymous";
  static final String HEALTH_CHECK_PATH = "/health-check";

  private static final Logger log = LoggerFactory.getLogger(NoiseWebSocketTunnelServer.class);

  public NoiseWebSocketTunnelServer(final int websocketPort,
      @Nullable final X509Certificate[] tlsCertificateChain,
      @Nullable final PrivateKey tlsPrivateKey,
      final NioEventLoopGroup eventLoopGroup,
      final Executor delegatedTaskExecutor,
      final GrpcClientConnectionManager grpcClientConnectionManager,
      final ClientPublicKeysManager clientPublicKeysManager,
      final ECKeyPair ecKeyPair,
      final LocalAddress authenticatedGrpcServerAddress,
      final LocalAddress anonymousGrpcServerAddress,
      final String recognizedProxySecret) throws SSLException {

    @Nullable final SslContext sslContext;

    if (tlsCertificateChain != null && tlsPrivateKey != null) {
      final SslProvider sslProvider;

      if (OpenSsl.isAvailable()) {
        log.info("Native OpenSSL provider is available; will use native provider");
        sslProvider = SslProvider.OPENSSL;
      } else {
        log.info("No native SSL provider available; will use JDK provider");
        sslProvider = SslProvider.JDK;
      }

      sslContext = SslContextBuilder.forServer(tlsPrivateKey, tlsCertificateChain)
          .clientAuth(ClientAuth.NONE)
          // Some load balancers require TLS 1.2 for health checks
          .protocols(SslProtocols.TLS_v1_3, SslProtocols.TLS_v1_2)
          .sslProvider(sslProvider)
          .build();
    } else {
      log.warn("No TLS credentials provided; Noise-over-WebSocket tunnel will not use TLS. This configuration is not suitable for production environments.");
      sslContext = null;
    }

    this.bootstrap = new ServerBootstrap()
        .group(eventLoopGroup)
        .channel(NioServerSocketChannel.class)
        .localAddress(websocketPort)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel socketChannel) {
            socketChannel.pipeline()
                .addLast(new ProxyProtocolDetectionHandler())
                .addLast(new HAProxyMessageHandler());

            if (sslContext != null) {
              socketChannel.pipeline().addLast(sslContext.newHandler(socketChannel.alloc(), delegatedTaskExecutor));
            }

            socketChannel.pipeline()
                .addLast(new HttpServerCodec())
                .addLast(new HttpObjectAggregator(Noise.MAX_PACKET_LEN))
                // The WebSocket opening handshake handler will remove itself from the pipeline once it has received a valid WebSocket upgrade
                // request and passed it down the pipeline
                .addLast(new WebSocketOpeningHandshakeHandler(AUTHENTICATED_SERVICE_PATH, ANONYMOUS_SERVICE_PATH, HEALTH_CHECK_PATH))
                .addLast(new WebSocketServerProtocolHandler("/", true))
                // Metrics on inbound/outbound Close frames
                .addLast(new WebSocketCloseMetricHandler())
                // Turn generic OutboundCloseErrorMessages into websocket close frames
                .addLast(new WebSocketOutboundErrorHandler())
                .addLast(new RejectUnsupportedMessagesHandler())
                .addLast(new WebsocketPayloadCodec())
                // The WebSocket handshake complete listener will forward the first payload supplemented with
                // data from the websocket handshake completion event, and then remove itself from the pipeline
                .addLast(new WebsocketHandshakeCompleteHandler(recognizedProxySecret))
                // The NoiseHandshakeHandler will perform the noise handshake and then replace itself with a
                // NoiseHandler
                .addLast(new NoiseHandshakeHandler(clientPublicKeysManager, ecKeyPair))
                // This handler will open a local connection to the appropriate gRPC server and install a ProxyHandler
                // once the Noise handshake has completed
                .addLast(new EstablishLocalGrpcConnectionHandler(
                    grpcClientConnectionManager,
                    authenticatedGrpcServerAddress, anonymousGrpcServerAddress,
                    FramingType.WEBSOCKET))
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
