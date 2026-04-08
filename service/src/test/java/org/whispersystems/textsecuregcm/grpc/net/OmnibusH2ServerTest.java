package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleUserEventChannelHandler;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioChannelOption;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageEncoder;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2GoAwayFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.DefaultHttp2ResetFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2ResetFrame;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ReferenceCountUtil;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OmnibusH2ServerTest extends AbstractLeakDetectionTest {
  private static final String KEYSTORE_PASSWORD = "password";

  // Paths that start with PREFIX should go to the prefix backend, everything else to default.
  private static final String PREFIX_BACKEND_IDENTITY = "prefix-backend";
  private static final String PREFIX = "/v1/prefix";
  private static final String DEFAULT_BACKEND_IDENTITY = "default-backend";

  private final NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup();
  private final DefaultEventLoopGroup localEventLoopGroup = new DefaultEventLoopGroup();

  private Channel defaultBackend;
  private Channel prefixBackend;
  private CompletableFuture<Channel> backendConnection;

  private OmnibusH2Server server;

  @BeforeEach
  void setUp() throws Exception {
    // Start two H2C backend servers that echo a response with their identity
    defaultBackend = startH2CServer(true, DEFAULT_BACKEND_IDENTITY);
    prefixBackend = startH2CServer(false, PREFIX_BACKEND_IDENTITY);
    backendConnection = new CompletableFuture<>();

    // self-signed TLS context for the frontend loaded from test keyStore
    final InputStream keyStore = OmnibusH2ServerTest.class.getResourceAsStream("omnibus-h2-server-test-keystore.p12");

    server = new OmnibusH2Server(
        SniMapper.buildSniMapping(keyStore, KEYSTORE_PASSWORD),
        nioEventLoopGroup,
        localEventLoopGroup,
        new InetSocketAddress("127.0.0.1", 0),
        new OmnibusRouter(
            List.of(new OmnibusRouter.OmnibusRoute(PREFIX, prefixBackend.localAddress())),
            defaultBackend.localAddress()),
        Duration.ofMinutes(1));

    server.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    server.stop();
    defaultBackend.close().sync();
    prefixBackend.close().sync();
    localEventLoopGroup.shutdownGracefully(1, 1000, TimeUnit.MILLISECONDS).sync();
    nioEventLoopGroup.shutdownGracefully(1, 1000, TimeUnit.MILLISECONDS).sync();
  }


  @Test
  void defaultBackend() {
    final String response = sendRequestThroughOmnibus("/a/different/path");
    assertEquals(DEFAULT_BACKEND_IDENTITY, response);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void forwardedForHeader(final boolean usePpv2) {
    final String expectedSource = usePpv2 ? "127.0.0.123" : "127.0.0.1";
    final HAProxyMessage proxyMessage = usePpv2
        ? new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4, expectedSource, "127.0.0.2", 1234, 5678)
        : null;
    final Channel h2Connection = connectToOmnibus(null, proxyMessage);
    final String xForwardedFor = sendRequestThroughOmnibus(h2Connection, "/forwarded-for");
    assertEquals(expectedSource, xForwardedFor);
  }

  @ParameterizedTest
  @ValueSource(strings = {"/v1/prefix", "/v1/prefix/", "/v1/prefix/other"})
  void prefixBackend(final String path) {
    final String response = sendRequestThroughOmnibus(path);
    assertEquals(PREFIX_BACKEND_IDENTITY, response);
  }

  @Test
  void multipleStreamsOnSameConnection() {
    final Channel h2Connection = connectToOmnibus();
    final int numStreams = 10;

    // Create concurrent streams to both backends on the same connection simultaneously
    @SuppressWarnings("rawtypes")
    final CompletableFuture[] futures = IntStream.range(0, numStreams)
        .mapToObj(i -> CompletableFuture.supplyAsync(() ->
            sendRequestThroughOmnibus(
                h2Connection,
                i % 2 == 0 ? PREFIX : "/v1/other")))
        .toArray(CompletableFuture[]::new);

    // Ensure we get the response from the correct backend for each stream
    CompletableFuture.allOf(futures).join();
    for (int i = 0; i < numStreams; i++) {
      assertEquals(
          i % 2 == 0 ? PREFIX_BACKEND_IDENTITY : DEFAULT_BACKEND_IDENTITY,
          futures[i].resultNow());
    }
  }

  @Test
  void backendDownStreamReset() {
    // Kill the default backend so connection attempts from the omnibus fail
    defaultBackend.close().syncUninterruptibly();

    final Channel h2Connection = connectToOmnibus();
    final CompletableFuture<Http2HeadersFrame> headersFuture = new CompletableFuture<>();
    final Http2StreamChannel stream = new Http2StreamChannelBootstrap(h2Connection)
        .handler(new HeadersCollectorHandler(headersFuture))
        .open()
        .syncUninterruptibly()
        .getNow();

    final Http2Headers headers = new DefaultHttp2Headers()
        .method("POST")
        .path("/test")
        .scheme("https")
        .authority("localhost");
    stream.writeAndFlush(new DefaultHttp2HeadersFrame(headers, true));
    final Http2HeadersFrame responseHeaders = headersFuture.join();
    assertEquals("502", responseHeaders.headers().status().toString());

    // Stream is dead, but connection should stay alive
    assertFalse(
        h2Connection.closeFuture().awaitUninterruptibly(5, TimeUnit.MILLISECONDS),
        "connection should stay open");
    assertTrue(h2Connection.isOpen());

    h2Connection.close().syncUninterruptibly();
  }

  @ParameterizedTest
  @ValueSource(strings = {"/goaway", "/reset"})
  void backendCloseClosesClientStream(final String path) {
    final Channel h2Connection = connectToOmnibus();
    final CompletableFuture<Http2ResetFrame> resetFuture = new CompletableFuture<>();
    final Http2StreamChannel stream = new Http2StreamChannelBootstrap(h2Connection)
        .handler(new RstCollectorHandler(resetFuture))
        .open()
        .syncUninterruptibly()
        .getNow();

    final Http2Headers headers = new DefaultHttp2Headers()
        .method("POST")
        // Triggers the server stream handler to either GOAWAY+close or send an RST based on the path
        .path(path)
        .scheme("https")
        .authority("localhost");
    stream.writeAndFlush(new DefaultHttp2HeadersFrame(headers, true));
    assertEquals(Http2Error.CANCEL.code(), resetFuture.join().errorCode());

    // client<->omnibus h2 connection stays open after a backend close/rst
    assertTrue(h2Connection.isActive());
  }

  @Test
  void queuedDataFrames() throws Exception {
    final Channel h2Connection = connectToOmnibus();
    final CompletableFuture<String> responseFuture = new CompletableFuture<>();
    final Http2StreamChannel stream = new Http2StreamChannelBootstrap(h2Connection)
        .handler(new ResponseCollectorHandler(responseFuture))
        .open()
        .syncUninterruptibly()
        .getNow();

    final Http2Headers headers = new DefaultHttp2Headers()
        .method("POST")
        .path("/test")
        .scheme("https")
        .authority("localhost");

    final int numFrames = 64;

    // Omnibus should handle queueing up frames while connecting to the backend if we blast all the frames right away
    stream.write(new DefaultHttp2HeadersFrame(headers, false));
    final StringBuilder expectedBuilder = new StringBuilder();
    for (int i = 0; i < numFrames; i++) {
      final String chunk = String.format("chunk-%03d;", i);
      expectedBuilder.append(chunk);
      final boolean endStream = (i == numFrames - 1);
      stream.write(new DefaultHttp2DataFrame(
          Unpooled.copiedBuffer(chunk, StandardCharsets.UTF_8), endStream));
    }
    stream.flush();
    final String expected = expectedBuilder.toString();

    final String response = responseFuture.get(10, TimeUnit.SECONDS);
    assertEquals(expected, response);

    h2Connection.close().syncUninterruptibly();
  }

  @Test
  void clientDisconnectClosesBackendConnections() throws Exception {
    final Channel h2Connection = connectToOmnibus();

    final Http2StreamChannelBootstrap streamBootstrap = new Http2StreamChannelBootstrap(h2Connection);
    final Http2StreamChannel stream = streamBootstrap
        .handler(new ResponseCollectorHandler(new CompletableFuture<>()))
        .open()
        .syncUninterruptibly()
        .getNow();

    // Write an endStream=false header so the stream stays open
    final Http2Headers headers = new DefaultHttp2Headers()
        .method("POST")
        .path("/test")
        .scheme("https")
        .authority("localhost");
    stream.writeAndFlush(new DefaultHttp2HeadersFrame(headers, false)).syncUninterruptibly();
    final Channel backendServerChannel = backendConnection.join();
    assertFalse(
        backendServerChannel.closeFuture().awaitUninterruptibly(10, TimeUnit.MILLISECONDS),
        "Channel should be open");

    // All backend connections the omnibus opened on behalf of this client should close if we disconnect the client
    h2Connection.close().syncUninterruptibly();
    assertTrue(backendServerChannel.closeFuture().await(5, TimeUnit.SECONDS));
  }


  @Test
  void backpressure() throws ExecutionException, InterruptedException, TimeoutException {
    final Channel h2Connection = connectToOmnibus();

    // We'll take the client channel becoming unwritable as backpressure signal
    final AtomicBoolean isWritable = new AtomicBoolean(true);
    final CompletableFuture<Http2HeadersFrame> response = new CompletableFuture<>();
    final Http2StreamChannel stream = new Http2StreamChannelBootstrap(h2Connection)
        .handler(new ChannelInboundHandlerAdapter() {
          @Override
          public void channelWritabilityChanged(ChannelHandlerContext ctx) {
            isWritable.set(ctx.channel().isWritable());
          }

          @Override
          public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
              if (msg instanceof Http2HeadersFrame headers) {
                response.complete(headers);
              }
            } finally {
              ReferenceCountUtil.release(msg);
            }
          }
        })
        .open()
        .syncUninterruptibly()
        .getNow();

    final Http2Headers headers = new DefaultHttp2Headers().method("POST").path("/test");
    stream.writeAndFlush(new DefaultHttp2HeadersFrame(headers, false)).syncUninterruptibly();

    // Make the backend H2 stream processor 'slow' by disabling auto-read on it
    final Channel backendServerChannel = backendConnection.join();
    backendServerChannel.config().setAutoRead(false);

    final byte[] chunk = new byte[16384];
    do {
      // Write data until our own client hits the high watermark
      while (isWritable.get()) {
        // Try to wait until the write finishes but if it can't that's fine: we're trying to induce backpressure
        stream
            .writeAndFlush(new DefaultHttp2DataFrame(Unpooled.wrappedBuffer(chunk), false))
            .awaitUninterruptibly(100, TimeUnit.MILLISECONDS);
        Thread.yield();
      }

      // Make sure our channel is still unwritable for a bit since we haven't re-enabled auto-read yet. If we become
      // writable it means we are hitting a lower watermark somewhere earlier in the stack, so we can try writing some
      // more. Eventually all intermediate channels should flush and we should be stuck on the backend channel which
      // will never make progress (because auto-read is disabled)
      Thread.sleep(100);
    } while (isWritable.get());
    stream.writeAndFlush(new DefaultHttp2DataFrame(Unpooled.wrappedBuffer(chunk), true));

    // Now re-enable reads on the backend, which should eventually unblock our writes
    backendConnection.resultNow().config().setAutoRead(true);
    // Now we should eventually be able to send the last (endStream=true) write and get a response
    assertEquals("200", response.get(5, TimeUnit.SECONDS).headers().status().toString());
    assertTrue(isWritable.get());

    h2Connection.close().syncUninterruptibly();
  }

  @Test
  void idleTest() throws Exception {
    final InputStream keyStore = OmnibusH2ServerTest.class.getResourceAsStream("omnibus-h2-server-test-keystore.p12");
    final Duration timeout = Duration.ofMillis(500);
    final OmnibusH2Server timeoutServer = new OmnibusH2Server(
        SniMapper.buildSniMapping(keyStore, KEYSTORE_PASSWORD),
        nioEventLoopGroup,
        localEventLoopGroup,
        new InetSocketAddress("127.0.0.1", 0),
        new OmnibusRouter(
            List.of(new OmnibusRouter.OmnibusRoute(PREFIX, prefixBackend.localAddress())),
            defaultBackend.localAddress()),
        timeout);
    timeoutServer.start();

    final Channel channel = connectToOmnibus(timeoutServer, null);

    // Send a request to make sure idleTimeouts work even after a stream / backend connection has been established
    sendRequestThroughOmnibus(channel, "/a/different/path");

    // The server should eventually close this idle connection
    assertTrue(channel.closeFuture().awaitUninterruptibly(timeout.toMillis() * 5, TimeUnit.MILLISECONDS));

    timeoutServer.stop();
  }

  private Channel startH2CServer(final boolean local, final String identity) throws InterruptedException {
    final EventLoopGroup eventLoopGroup = local ? localEventLoopGroup : nioEventLoopGroup;
    return new ServerBootstrap()
        .group(eventLoopGroup, eventLoopGroup)
        .channel(local ? LocalServerChannel.class : NioServerSocketChannel.class)
        // Limit size of kernel TCP buffers to make it easier to hit backpressure in tests
        .option(NioChannelOption.SO_RCVBUF, 8192)
        .option(NioChannelOption.SO_SNDBUF, 8192)
        .childHandler(new ChannelInitializer<>() {
          @Override
          protected void initChannel(final Channel ch) {
            backendConnection.complete(ch);
            ch.pipeline().addLast(Http2FrameCodecBuilder.forServer().build());
            ch.pipeline().addLast(new Http2MultiplexHandler(new ChannelInitializer<Http2StreamChannel>() {
              @Override
              protected void initChannel(final Http2StreamChannel ch) {
                ch.pipeline().addLast(new TestHandler(identity));
              }
            }));
          }
        })
        .bind(local ? new LocalAddress(identity) : new InetSocketAddress("127.0.0.1", 0))
        .sync()
        .channel();
  }

  private Channel connectToOmnibus() {
    return connectToOmnibus(null, null);
  }

  /// Makes an H2 connection to the omnibus at [this#server] on which new H2 streams can be opened
  private Channel connectToOmnibus(@Nullable OmnibusH2Server server, @Nullable final HAProxyMessage proxyHeader) {
    if (server == null) {
      server = this.server;
    }
    final SslContext clientSsl;
    try {
      clientSsl = SslContextBuilder.forClient()
          .trustManager(InsecureTrustManagerFactory.INSTANCE)
          .applicationProtocolConfig(new ApplicationProtocolConfig(
              ApplicationProtocolConfig.Protocol.ALPN,
              ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
              ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
              ApplicationProtocolNames.HTTP_2))
          .build();
    } catch (SSLException e) {
      throw new RuntimeException(e);
    }

    final Bootstrap clientBootstrap = new Bootstrap()
        .group(nioEventLoopGroup)
        .channel(NioSocketChannel.class)
        // Limit size of kernel TCP buffers to make it easier to hit backpressure in tests
        .option(NioChannelOption.SO_RCVBUF, 8192)
        .option(NioChannelOption.SO_SNDBUF, 8192)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(final SocketChannel ch) {
            ch.pipeline().addLast(HAProxyMessageEncoder.INSTANCE);
          }
        });
    final Channel ch = clientBootstrap.connect(server.getLocalAddress())
        .syncUninterruptibly()
        .channel();
    if (proxyHeader != null) {
      ch.writeAndFlush(proxyHeader).syncUninterruptibly();
    }
    ch.pipeline().remove(HAProxyMessageEncoder.INSTANCE);
    ch.pipeline().addLast(clientSsl.newHandler(ch.alloc(), server.getLocalAddress().getHostName(), server.getLocalAddress().getPort()));
    ch.pipeline().addLast(Http2FrameCodecBuilder.forClient()
        .initialSettings(Http2Settings.defaultSettings())
        .build());
    ch.pipeline().addLast(new Http2MultiplexHandler(new ChannelInboundHandlerAdapter()));
    return ch;
  }

  /// Sends an H2 request to [this#server] and returns the H2 response body
  private String sendRequestThroughOmnibus(final String path) {
    final Channel h2Connection = connectToOmnibus();
    final String result = sendRequestThroughOmnibus(h2Connection, path);
    h2Connection.close().syncUninterruptibly();
    return result;
  }

  private String sendRequestThroughOmnibus(final Channel h2Connection, final String path) {
    final CompletableFuture<String> responseFuture = new CompletableFuture<>();
    final Http2StreamChannelBootstrap streamBootstrap = new Http2StreamChannelBootstrap(h2Connection);
    final Http2StreamChannel stream = streamBootstrap
        .handler(new ResponseCollectorHandler(responseFuture))
        .open()
        .syncUninterruptibly()
        .getNow();

    final Http2Headers headers = new DefaultHttp2Headers()
        .method("POST")
        .path(path)
        .scheme("https")
        .authority("localhost");

    stream.writeAndFlush(new DefaultHttp2HeadersFrame(headers, true));

    return responseFuture.join();
  }

  /// A backend that either echos the request body, returns an identity, or disconnects based on the request
  private static class TestHandler extends ChannelInboundHandlerAdapter {

    // Returned if request has no body
    private final String identity;
    private final ByteBuf accumulated = Unpooled.buffer();

    private TestHandler(final String identity) {
      this.identity = identity;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
      if (msg instanceof Http2HeadersFrame headers) {
        final String path = headers.headers().path().toString();
        if (path.contains("reset")) {
          ctx.writeAndFlush(new DefaultHttp2ResetFrame(Http2Error.NO_ERROR))
              .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        } else if (path.contains("goaway")) {
          ctx.channel().parent()
              .writeAndFlush(new DefaultHttp2GoAwayFrame(Http2Error.NO_ERROR))
              .addListener(ChannelFutureListener.CLOSE);
        } else if (path.contains("forwarded-for")) {
          final String xForwardedFor = Optional
              .ofNullable(headers.headers().get("x-forwarded-for"))
              .map(CharSequence::toString)
              .orElse("");
          writeResponse(ctx, Unpooled.copiedBuffer(xForwardedFor, StandardCharsets.UTF_8));
        } else if (headers.isEndStream()) {
          writeResponse(ctx, Unpooled.copiedBuffer(identity, StandardCharsets.UTF_8));
        }
      } else if (msg instanceof Http2DataFrame dataFrame) {
        accumulated.writeBytes(dataFrame.content());
        if (dataFrame.isEndStream()) {
          writeResponse(ctx, accumulated);
        }
      }
      ReferenceCountUtil.release(msg);
    }

    private void writeResponse(final ChannelHandlerContext ctx, final ByteBuf body) {
      final Http2Headers responseHeaders = new DefaultHttp2Headers().status("200");
      ctx.write(new DefaultHttp2HeadersFrame(responseHeaders, false));
      ctx.writeAndFlush(new DefaultHttp2DataFrame(body, true));
    }
  }

  /// Completes the provided future with the first [Http2DataFrame] received
  private static class ResponseCollectorHandler extends ChannelInboundHandlerAdapter {

    private final CompletableFuture<String> responseFuture;

    ResponseCollectorHandler(final CompletableFuture<String> responseFuture) {
      this.responseFuture = responseFuture;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
      if (msg instanceof Http2DataFrame dataFrame) {
        responseFuture.complete(dataFrame.content().toString(StandardCharsets.UTF_8));
      }
      ReferenceCountUtil.release(msg);
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      responseFuture.completeExceptionally(cause);
    }
  }

  /// Completes the provided future with the first [Http2HeadersFrame] received
  private static class HeadersCollectorHandler extends ChannelInboundHandlerAdapter {

    private final CompletableFuture<Http2HeadersFrame> responseFuture;

    HeadersCollectorHandler(final CompletableFuture<Http2HeadersFrame> responseFuture) {
      this.responseFuture = responseFuture;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
      if (msg instanceof Http2HeadersFrame headers) {
        responseFuture.complete(headers);
      }
      ReferenceCountUtil.release(msg);
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      responseFuture.completeExceptionally(cause);
    }
  }

  /// Completes the provided future when an RST frame is received or errors if we don't get one
  private static class RstCollectorHandler extends SimpleUserEventChannelHandler<Http2ResetFrame> {

    private final CompletableFuture<Http2ResetFrame> resetFuture;

    private RstCollectorHandler(final CompletableFuture<Http2ResetFrame> resetFuture) {
      this.resetFuture = resetFuture;
    }

    @Override
    protected void eventReceived(final ChannelHandlerContext ctx, final Http2ResetFrame evt) {
      resetFuture.complete(evt);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
      if (!resetFuture.isDone()) {
        resetFuture.completeExceptionally(new IllegalStateException("Channel went inactive without RST"));
      }
    }
  }
}
