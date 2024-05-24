package org.whispersystems.textsecuregcm.grpc.net;

import com.southernstorm.noise.protocol.Noise;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageEncoder;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.ReferenceCountUtil;
import java.net.SocketAddress;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;

class EstablishRemoteConnectionHandler extends ChannelInboundHandlerAdapter {

  private final boolean useTls;
  @Nullable private final X509Certificate trustedServerCertificate;
  private final URI websocketUri;
  private final boolean authenticated;
  @Nullable private final ECKeyPair ecKeyPair;
  private final ECPublicKey rootPublicKey;
  @Nullable private final UUID accountIdentifier;
  private final byte deviceId;
  private final HttpHeaders headers;
  private final SocketAddress remoteServerAddress;
  private final WebSocketCloseListener webSocketCloseListener;
  @Nullable private final Supplier<HAProxyMessage> proxyMessageSupplier;

  private final List<Object> pendingReads = new ArrayList<>();

  private static final String NOISE_HANDSHAKE_HANDLER_NAME = "noise-handshake";

  EstablishRemoteConnectionHandler(
      final boolean useTls,
      @Nullable final X509Certificate trustedServerCertificate,
      final URI websocketUri,
      final boolean authenticated,
      @Nullable final ECKeyPair ecKeyPair,
      final ECPublicKey rootPublicKey,
      @Nullable final UUID accountIdentifier,
      final byte deviceId,
      final HttpHeaders headers,
      final SocketAddress remoteServerAddress,
      final WebSocketCloseListener webSocketCloseListener,
      @Nullable Supplier<HAProxyMessage> proxyMessageSupplier) {

    this.useTls = useTls;
    this.trustedServerCertificate = trustedServerCertificate;
    this.websocketUri = websocketUri;
    this.authenticated = authenticated;
    this.ecKeyPair = ecKeyPair;
    this.rootPublicKey = rootPublicKey;
    this.accountIdentifier = accountIdentifier;
    this.deviceId = deviceId;
    this.headers = headers;
    this.remoteServerAddress = remoteServerAddress;
    this.webSocketCloseListener = webSocketCloseListener;
    this.proxyMessageSupplier = proxyMessageSupplier;
  }

  @Override
  public void handlerAdded(final ChannelHandlerContext localContext) {
    new Bootstrap()
        .channel(NioSocketChannel.class)
        .group(localContext.channel().eventLoop())
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(final SocketChannel channel) throws SSLException {

            if (proxyMessageSupplier != null) {
              // In a production setting, we'd want some mechanism to remove these handlers after the initial message
              // were sent. Since this is just for testing, though, we can tolerate the inefficiency of leaving a
              // pair of inert handlers in the pipeline.
              channel.pipeline()
                  .addLast(HAProxyMessageEncoder.INSTANCE)
                  .addLast(new HAProxyMessageSender(proxyMessageSupplier));
            }

            if (useTls) {
              final SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();

              if (trustedServerCertificate != null) {
                sslContextBuilder.trustManager(trustedServerCertificate);
              }

              channel.pipeline().addLast(sslContextBuilder.build().newHandler(channel.alloc()));
            }

            channel.pipeline()
                .addLast(new HttpClientCodec())
                .addLast(new HttpObjectAggregator(Noise.MAX_PACKET_LEN))
                // Inbound CloseWebSocketFrame messages wil get "eaten" by the WebSocketClientProtocolHandler, so if we
                // want to react to them on our own, we need to catch them before they hit that handler.
                .addLast(new InboundCloseWebSocketFrameHandler(webSocketCloseListener))
                .addLast(new WebSocketClientProtocolHandler(websocketUri,
                    WebSocketVersion.V13,
                    null,
                    false,
                    headers,
                    Noise.MAX_PACKET_LEN,
                    10_000))
                .addLast(new OutboundCloseWebSocketFrameHandler(webSocketCloseListener))
                .addLast(authenticated
                    ? new NoiseXXClientHandshakeHandler(ecKeyPair, rootPublicKey, accountIdentifier, deviceId)
                    : new NoiseNXClientHandshakeHandler(rootPublicKey))
                .addLast(NOISE_HANDSHAKE_HANDLER_NAME, new ChannelInboundHandlerAdapter() {
                  @Override
                  public void userEventTriggered(final ChannelHandlerContext remoteContext, final Object event)
                      throws Exception {
                    if (event instanceof NoiseHandshakeCompleteEvent) {
                      remoteContext.pipeline()
                          .replace(NOISE_HANDSHAKE_HANDLER_NAME, null, new ProxyHandler(localContext.channel()));

                      localContext.pipeline().addLast(new ProxyHandler(remoteContext.channel()));

                      pendingReads.forEach(localContext::fireChannelRead);
                      pendingReads.clear();

                      localContext.pipeline().remove(EstablishRemoteConnectionHandler.this);
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
