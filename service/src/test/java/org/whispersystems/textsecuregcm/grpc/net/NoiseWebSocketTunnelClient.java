package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import java.net.SocketAddress;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;

class NoiseWebSocketTunnelClient implements AutoCloseable {

  private final ServerBootstrap serverBootstrap;
  private Channel serverChannel;

  static final URI AUTHENTICATED_WEBSOCKET_URI = URI.create("wss://localhost/authenticated");
  static final URI ANONYMOUS_WEBSOCKET_URI = URI.create("wss://localhost/anonymous");

  static class Builder {

    final SocketAddress remoteServerAddress;
    NioEventLoopGroup eventLoopGroup;
    ECPublicKey serverPublicKey;

    URI websocketUri = ANONYMOUS_WEBSOCKET_URI;
    HttpHeaders headers = new DefaultHttpHeaders();
    WebSocketCloseListener webSocketCloseListener = WebSocketCloseListener.NOOP_LISTENER;

    boolean authenticated = false;
    ECKeyPair ecKeyPair = null;
    UUID accountIdentifier = null;
    byte deviceId = 0x00;
    boolean useTls;
    X509Certificate trustedServerCertificate = null;
    Supplier<HAProxyMessage> proxyMessageSupplier = null;

    Builder(
        final SocketAddress remoteServerAddress,
        final NioEventLoopGroup eventLoopGroup,
        final ECPublicKey serverPublicKey) {
      this.remoteServerAddress = remoteServerAddress;
      this.eventLoopGroup = eventLoopGroup;
      this.serverPublicKey = serverPublicKey;
    }

    Builder setAuthenticated(final ECKeyPair ecKeyPair, final UUID accountIdentifier, final byte deviceId) {
      this.authenticated = true;
      this.accountIdentifier = accountIdentifier;
      this.deviceId = deviceId;
      this.ecKeyPair = ecKeyPair;
      this.websocketUri = AUTHENTICATED_WEBSOCKET_URI;
      return this;
    }

    Builder setWebsocketUri(final URI websocketUri) {
      this.websocketUri = websocketUri;
      return this;
    }

    Builder setUseTls(X509Certificate trustedServerCertificate) {
      this.useTls = true;
      this.trustedServerCertificate = trustedServerCertificate;
      return this;
    }

    Builder setProxyMessageSupplier(Supplier<HAProxyMessage> proxyMessageSupplier) {
      this.proxyMessageSupplier = proxyMessageSupplier;
      return this;
    }

    Builder setHeaders(final HttpHeaders headers) {
      this.headers = headers;
      return this;
    }

    Builder setWebSocketCloseListener(final WebSocketCloseListener webSocketCloseListener) {
      this.webSocketCloseListener = webSocketCloseListener;
      return this;
    }

    Builder setServerPublicKey(ECPublicKey serverPublicKey) {
      this.serverPublicKey = serverPublicKey;
      return this;
    }

    NoiseWebSocketTunnelClient build() {
      final NoiseWebSocketTunnelClient client =
          new NoiseWebSocketTunnelClient(eventLoopGroup, fastOpenRequest -> new EstablishRemoteConnectionHandler(
              useTls, trustedServerCertificate, websocketUri, authenticated, ecKeyPair, serverPublicKey,
              accountIdentifier, deviceId, headers, remoteServerAddress, webSocketCloseListener, proxyMessageSupplier,
              fastOpenRequest));
      client.start();
      return client;
    }
  }

  private NoiseWebSocketTunnelClient(NioEventLoopGroup eventLoopGroup,
      Function<byte[], EstablishRemoteConnectionHandler> handler) {

    this.serverBootstrap = new ServerBootstrap()
        .localAddress(new LocalAddress("websocket-noise-tunnel-client"))
        .channel(LocalServerChannel.class)
        .group(eventLoopGroup)
        .childHandler(new ChannelInitializer<LocalChannel>() {
          @Override
          protected void initChannel(final LocalChannel localChannel) {
            localChannel.pipeline()
                // We just get a bytestream out of the gRPC client, but we need to pull out the first "request" from the
                // stream to do a "fast-open" request. So we buffer HTTP/2 frames until we get a whole "request" to put
                // in the handshake.
                .addLast(Http2Buffering.handler())
                // Once we have a complete request we'll get an event and after bytes will start flowing as-is again. At
                // that point we can pass everything off to the EstablishRemoteConnectionHandler which will actually
                // connect to the remote service
                .addLast(new ChannelInboundHandlerAdapter() {
                  @Override
                  public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
                    if (evt instanceof FastOpenRequestBufferedEvent requestBufferedEvent) {
                      byte[] fastOpenRequest = ByteBufUtil.getBytes(requestBufferedEvent.fastOpenRequest());
                      requestBufferedEvent.fastOpenRequest().release();
                      ctx.pipeline().addLast(handler.apply(fastOpenRequest));
                    }
                    super.userEventTriggered(ctx, evt);
                  }
                })
                .addLast(new ClientErrorHandler());
          }
        });
  }


  LocalAddress getLocalAddress() {
    return (LocalAddress) serverChannel.localAddress();
  }

  private NoiseWebSocketTunnelClient start() {
    serverChannel = serverBootstrap.bind().awaitUninterruptibly().channel();
    return this;
  }

  @Override
  public void close() throws InterruptedException {
    serverChannel.close().await();
  }

}
