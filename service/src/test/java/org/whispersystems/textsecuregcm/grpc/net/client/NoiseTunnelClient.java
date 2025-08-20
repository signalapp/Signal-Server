package org.whispersystems.textsecuregcm.grpc.net.client;

import com.google.protobuf.ByteString;
import com.southernstorm.noise.protocol.Noise;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageEncoder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.ReferenceCountUtil;
import java.net.SocketAddress;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.net.ssl.SSLException;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.grpc.net.FramingType;
import org.whispersystems.textsecuregcm.grpc.net.NoiseTunnelProtos;
import org.whispersystems.textsecuregcm.grpc.net.noisedirect.NoiseDirectFrame;
import org.whispersystems.textsecuregcm.grpc.net.noisedirect.NoiseDirectFrameCodec;
import org.whispersystems.textsecuregcm.grpc.net.noisedirect.NoiseDirectProtos;
import org.whispersystems.textsecuregcm.grpc.net.websocket.WebsocketPayloadCodec;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class NoiseTunnelClient implements AutoCloseable {

  private final CompletableFuture<CloseFrameEvent> closeEventFuture;
  private final CompletableFuture<NoiseClientHandshakeCompleteEvent> handshakeEventFuture;
  private final CompletableFuture<Void> userCloseFuture;
  private final ServerBootstrap serverBootstrap;
  private Channel serverChannel;

  public static final URI AUTHENTICATED_WEBSOCKET_URI = URI.create("wss://localhost/authenticated");
  public static final URI ANONYMOUS_WEBSOCKET_URI = URI.create("wss://localhost/anonymous");

  public static class Builder {

    final SocketAddress remoteServerAddress;
    NioEventLoopGroup eventLoopGroup;
    ECPublicKey serverPublicKey;

    FramingType framingType = FramingType.WEBSOCKET;
    URI websocketUri = ANONYMOUS_WEBSOCKET_URI;
    HttpHeaders headers = new DefaultHttpHeaders();
    NoiseTunnelProtos.HandshakeInit.Builder handshakeInit = NoiseTunnelProtos.HandshakeInit.newBuilder();

    boolean authenticated = false;
    ECKeyPair ecKeyPair = null;
    boolean useTls;
    X509Certificate trustedServerCertificate = null;
    Supplier<HAProxyMessage> proxyMessageSupplier = null;

    public Builder(
        final SocketAddress remoteServerAddress,
        final NioEventLoopGroup eventLoopGroup,
        final ECPublicKey serverPublicKey) {
      this.remoteServerAddress = remoteServerAddress;
      this.eventLoopGroup = eventLoopGroup;
      this.serverPublicKey = serverPublicKey;
    }

    public Builder setAuthenticated(final ECKeyPair ecKeyPair, final UUID accountIdentifier, final byte deviceId) {
      this.authenticated = true;
      handshakeInit.setAci(UUIDUtil.toByteString(accountIdentifier));
      handshakeInit.setDeviceId(deviceId);
      this.ecKeyPair = ecKeyPair;
      this.websocketUri = AUTHENTICATED_WEBSOCKET_URI;
      return this;
    }

    public Builder setWebsocketUri(final URI websocketUri) {
      this.websocketUri = websocketUri;
      return this;
    }

    public Builder setUseTls(X509Certificate trustedServerCertificate) {
      this.useTls = true;
      this.trustedServerCertificate = trustedServerCertificate;
      return this;
    }

    public Builder setProxyMessageSupplier(Supplier<HAProxyMessage> proxyMessageSupplier) {
      this.proxyMessageSupplier = proxyMessageSupplier;
      return this;
    }

    public Builder setUserAgent(final String userAgent) {
      handshakeInit.setUserAgent(userAgent);
      return this;
    }

    public Builder setAcceptLanguage(final String acceptLanguage) {
      handshakeInit.setAcceptLanguage(acceptLanguage);
      return this;
    }

    public Builder setHeaders(final HttpHeaders headers) {
      this.headers = headers;
      return this;
    }

    public Builder setServerPublicKey(ECPublicKey serverPublicKey) {
      this.serverPublicKey = serverPublicKey;
      return this;
    }

    public Builder setFramingType(FramingType framingType) {
      this.framingType = framingType;
      return this;
    }

    public NoiseTunnelClient build() {
      final List<ChannelHandler> handlers = new ArrayList<>();
      if (proxyMessageSupplier != null) {
        handlers.addAll(List.of(HAProxyMessageEncoder.INSTANCE, new HAProxyMessageSender(proxyMessageSupplier)));
      }
      if (useTls) {
        final SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();

        if (trustedServerCertificate != null) {
          sslContextBuilder.trustManager(trustedServerCertificate);
        }

        try {
          handlers.add(sslContextBuilder.build().newHandler(ByteBufAllocator.DEFAULT));
        } catch (SSLException e) {
          throw new IllegalArgumentException(e);
        }
      }

      // handles the wrapping and unrwrapping the framing layer (websockets or noisedirect)
      handlers.addAll(switch (framingType) {
        case WEBSOCKET -> websocketHandlerStack(websocketUri, headers);
        case NOISE_DIRECT -> noiseDirectHandlerStack(authenticated);
      });

      final NoiseClientHandshakeHelper helper = authenticated
          ? NoiseClientHandshakeHelper.IK(serverPublicKey, ecKeyPair)
          : NoiseClientHandshakeHelper.NK(serverPublicKey);

      handlers.add(new NoiseClientHandshakeHandler(helper));

      // When the noise handshake completes we'll save the response from the server so client users can inspect it
      final UserEventFuture<NoiseClientHandshakeCompleteEvent> handshakeEventHandler =
          new UserEventFuture<>(NoiseClientHandshakeCompleteEvent.class);
      handlers.add(handshakeEventHandler);

      // Whenever the framing layer sends or receives a close frame, it will emit a CloseFrameEvent and we'll save off
      // information about why the connection was closed.
      final UserEventFuture<CloseFrameEvent> closeEventHandler = new UserEventFuture<>(CloseFrameEvent.class);
      handlers.add(closeEventHandler);

      // When the user closes the client, write a normal closure close frame
      final CompletableFuture<Void> userCloseFuture = new CompletableFuture<>();
      handlers.add(new ChannelInboundHandlerAdapter() {
        @Override
        public void handlerAdded(final ChannelHandlerContext ctx) {
          userCloseFuture.thenRunAsync(() -> ctx.pipeline().writeAndFlush(switch (framingType) {
                    case WEBSOCKET -> new CloseWebSocketFrame(WebSocketCloseStatus.NORMAL_CLOSURE);
                    case NOISE_DIRECT -> new NoiseDirectFrame(
                        NoiseDirectFrame.FrameType.CLOSE,
                        Unpooled.wrappedBuffer(NoiseDirectProtos.CloseReason
                            .newBuilder()
                            .setCode(NoiseDirectProtos.CloseReason.Code.OK)
                            .build()
                            .toByteArray()));
                  })
                  .addListener(ChannelFutureListener.CLOSE),
              ctx.executor());
        }
      });

      final NoiseTunnelClient client =
          new NoiseTunnelClient(eventLoopGroup, closeEventHandler.future, handshakeEventHandler.future, userCloseFuture, fastOpenRequest -> new EstablishRemoteConnectionHandler(
              handlers,
              remoteServerAddress,
              handshakeInit.setFastOpenRequest(ByteString.copyFrom(fastOpenRequest)).build()));
      client.start();
      return client;
    }
  }

  private NoiseTunnelClient(NioEventLoopGroup eventLoopGroup,
                            CompletableFuture<CloseFrameEvent> closeEventFuture,
                            CompletableFuture<NoiseClientHandshakeCompleteEvent> handshakeEventFuture,
                            CompletableFuture<Void> userCloseFuture,
                            Function<byte[], EstablishRemoteConnectionHandler> handler) {

    this.userCloseFuture = userCloseFuture;
    this.closeEventFuture = closeEventFuture;
    this.handshakeEventFuture = handshakeEventFuture;
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
                    if (evt instanceof FastOpenRequestBufferedEvent(ByteBuf fastOpenRequest)) {
                      byte[] fastOpenRequestBytes = ByteBufUtil.getBytes(fastOpenRequest);
                      fastOpenRequest.release();
                      ctx.pipeline().addLast(handler.apply(fastOpenRequestBytes));
                    }
                    super.userEventTriggered(ctx, evt);
                  }
                })
                .addLast(new ClientErrorHandler());
          }
        });
  }

  private static class UserEventFuture<T> extends ChannelInboundHandlerAdapter {
    private final CompletableFuture<T> future = new CompletableFuture<>();
    private final Class<T> cls;

    UserEventFuture(Class<T> cls) {
      this.cls = cls;
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
      if (cls.isInstance(evt)) {
        future.complete((T) evt);
      }
      ctx.fireUserEventTriggered(evt);
    }
  }


  public LocalAddress getLocalAddress() {
    return (LocalAddress) serverChannel.localAddress();
  }

  private NoiseTunnelClient start() {
    serverChannel = serverBootstrap.bind().awaitUninterruptibly().channel();
    return this;
  }

  @Override
  public void close() throws InterruptedException {
    userCloseFuture.complete(null);
    serverChannel.close().await();
  }

  /**
   * @return A future that completes when a close frame is observed
   */
  public CompletableFuture<CloseFrameEvent> closeFrameFuture() {
    return closeEventFuture;
  }

  /**
   * @return A future that completes when the noise handshake finishes
   */
  public CompletableFuture<NoiseClientHandshakeCompleteEvent> getHandshakeEventFuture() {
    return handshakeEventFuture;
  }


  private static List<ChannelHandler> noiseDirectHandlerStack(boolean authenticated) {
    return List.of(
        new LengthFieldBasedFrameDecoder(Noise.MAX_PACKET_LEN, 1, 2),
        new NoiseDirectFrameCodec(),
        new ChannelDuplexHandler() {
          @Override
          public void channelActive(ChannelHandlerContext ctx) {
            ctx.fireUserEventTriggered(new ReadyForNoiseHandshakeEvent());
            ctx.fireChannelActive();
          }

          @Override
          public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof NoiseDirectFrame ndf && ndf.frameType() == NoiseDirectFrame.FrameType.CLOSE) {
              try {
                final NoiseDirectProtos.CloseReason closeReason =
                    NoiseDirectProtos.CloseReason.parseFrom(ByteBufUtil.getBytes(ndf.content()));
                ctx.fireUserEventTriggered(
                    CloseFrameEvent.fromNoiseDirectCloseFrame(closeReason, CloseFrameEvent.CloseInitiator.SERVER));
              } finally {
                ReferenceCountUtil.release(msg);
              }
            } else {
              ctx.fireChannelRead(msg);
            }
          }

          @Override
          public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof NoiseDirectFrame ndf && ndf.frameType() == NoiseDirectFrame.FrameType.CLOSE) {
              final NoiseDirectProtos.CloseReason errorPayload =
                  NoiseDirectProtos.CloseReason.parseFrom(ByteBufUtil.getBytes(ndf.content()));
              ctx.fireUserEventTriggered(
                  CloseFrameEvent.fromNoiseDirectCloseFrame(errorPayload, CloseFrameEvent.CloseInitiator.CLIENT));
            }
            ctx.write(msg, promise);
          }
        },
        new MessageToMessageCodec<NoiseDirectFrame, ByteBuf>() {
          boolean noiseHandshakeFinished = false;

          @Override
          protected void encode(final ChannelHandlerContext ctx, final ByteBuf msg, final List<Object> out) {
            final NoiseDirectFrame.FrameType frameType = noiseHandshakeFinished
                ? NoiseDirectFrame.FrameType.DATA
                : (authenticated ? NoiseDirectFrame.FrameType.IK_HANDSHAKE : NoiseDirectFrame.FrameType.NK_HANDSHAKE);
            noiseHandshakeFinished = true;
            out.add(new NoiseDirectFrame(frameType, msg.retain()));
          }

          @Override
          protected void decode(final ChannelHandlerContext ctx, final NoiseDirectFrame msg,
                                final List<Object> out) {
            out.add(msg.content().retain());
          }
        });
  }

  private static List<ChannelHandler> websocketHandlerStack(final URI websocketUri, final HttpHeaders headers) {
    return List.of(
        new HttpClientCodec(),
        new HttpObjectAggregator(Noise.MAX_PACKET_LEN),
        // Inbound CloseWebSocketFrame messages wil get "eaten" by the WebSocketClientProtocolHandler, so if we
        // want to react to them on our own, we need to catch them before they hit that handler.
        new ChannelInboundHandlerAdapter() {
          @Override
          public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
            if (message instanceof CloseWebSocketFrame closeWebSocketFrame) {
              context.fireUserEventTriggered(
                  CloseFrameEvent.fromWebsocketCloseFrame(closeWebSocketFrame, CloseFrameEvent.CloseInitiator.SERVER));
            }

            super.channelRead(context, message);
          }
        },
        new WebSocketClientProtocolHandler(websocketUri,
            WebSocketVersion.V13,
            null,
            false,
            headers,
            Noise.MAX_PACKET_LEN,
            10_000),
        new ChannelOutboundHandlerAdapter() {
          @Override
          public void write(final ChannelHandlerContext context, final Object message, final ChannelPromise promise) throws Exception {
            if (message instanceof CloseWebSocketFrame closeWebSocketFrame) {
              context.fireUserEventTriggered(
                  CloseFrameEvent.fromWebsocketCloseFrame(closeWebSocketFrame, CloseFrameEvent.CloseInitiator.CLIENT));
            }
            super.write(context, message, promise);
          }
        },
        new ChannelInboundHandlerAdapter() {
          @Override
          public void userEventTriggered(final ChannelHandlerContext context, final Object event) {
            if (event instanceof WebSocketClientProtocolHandler.ClientHandshakeStateEvent clientHandshakeStateEvent) {
              if (clientHandshakeStateEvent == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                context.fireUserEventTriggered(new ReadyForNoiseHandshakeEvent());
              }
            }
            context.fireUserEventTriggered(event);
          }
        },
        new WebsocketPayloadCodec());
  }
}
