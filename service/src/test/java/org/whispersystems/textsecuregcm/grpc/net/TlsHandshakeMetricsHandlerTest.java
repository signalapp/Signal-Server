/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.ssl.SniHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;
import io.netty.util.Mapping;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TlsHandshakeMetricsHandlerTest {

  private static final String DOMAIN = "example.org";

  private static MultithreadEventLoopGroup eventLoopGroup;
  private static byte[] keyStoreBytes;
  private static String keyStorePassword;

  private SimpleMeterRegistry meterRegistry;
  private SslContext clientSslContext;
  private Channel serverChannel;

  /// Completed by the server once the handshake event has passed the metrics handler
  private CompletableFuture<SslHandshakeCompletionEvent> serverHandshakeFuture;

  @BeforeAll
  static void setUpBeforeAll() throws Exception {
    final Instant now = Instant.now();
    final X509Bundle x509Bundle = new CertificateBuilder()
        .notBefore(now)
        .notAfter(now.plus(Duration.ofHours(1)))
        .setIsCertificateAuthority(true)
        .algorithm(CertificateBuilder.Algorithm.ed25519)
        .subject("CN=" + DOMAIN)
        .addSanDnsName(DOMAIN)
        .buildSelfSigned();

    final char[] password = RandomStringUtils.insecure().nextAlphanumeric(16).toCharArray();
    final KeyStore keyStore = x509Bundle.toKeyStore(password);
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    keyStore.store(byteArrayOutputStream, password);

    keyStoreBytes = byteArrayOutputStream.toByteArray();
    keyStorePassword = new String(password);

    eventLoopGroup = new DefaultEventLoopGroup();
  }

  @BeforeEach
  void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();

    clientSslContext = SslContextBuilder.forClient()
        .sslProvider(SslProvider.JDK)
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .protocols("TLSv1.3")
        .build();
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    if (serverChannel != null) {
      serverChannel.close().sync();
    }
  }

  @AfterAll
  static void tearDownAfterAll() throws InterruptedException {
    eventLoopGroup.shutdownGracefully(0, 1000, TimeUnit.MILLISECONDS).sync();
  }

  @Test
  void handshake() throws Exception {
    final AtomicReference<Channel> lastServerChildChannel = startServer(buildSniMapping());
    connect(null);

    assertEquals(1, getCount(true));
    assertEquals(1, getTotalCount());

    assertNull(lastServerChildChannel.get().pipeline().get(TlsHandshakeMetricsHandler.class), "the metrics handler should remove itself");
  }

  @Test
  void failedHandshake() throws Exception {
    startServer(buildSniMapping());
    // The server only holds an Ed25519 key
    connect(new String[] { "rsa_pss_rsae_sha256" });

    assertFalse(serverHandshakeFuture.get(5, TimeUnit.SECONDS).isSuccess());
    assertEquals(1, getTotalCount());
    assertEquals(1, getCount(false));
  }

  private static Mapping<String, SslContext> buildSniMapping() throws Exception {
    return SniMapper.buildSniMapping(new ByteArrayInputStream(keyStoreBytes), keyStorePassword);
  }

  /// Starts the server and returns a reference to the child channel, for introspection by tests
  private AtomicReference<Channel> startServer(final Mapping<String, SslContext> sniMapping) throws Exception {
    final TlsHandshakeMetricsHandler metricsHandler = new TlsHandshakeMetricsHandler(meterRegistry);

    final AtomicReference<Channel> serverChildChannel = new AtomicReference<>();
    serverChannel = new ServerBootstrap()
        .group(eventLoopGroup)
        .channel(LocalServerChannel.class)
        .childHandler(new ChannelInitializer<>() {
          @Override
          protected void initChannel(final Channel ch) {
            serverChildChannel.set(ch);
            ch.pipeline().addLast(new SniHandler(sniMapping));
            ch.pipeline().addLast(metricsHandler);
            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
              @Override
              public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
                if (evt instanceof SslHandshakeCompletionEvent handshakeCompletionEvent) {
                  serverHandshakeFuture.complete(handshakeCompletionEvent);

                  // Send a byte so that the client channelRead() succeeds
                  if (handshakeCompletionEvent.isSuccess()) {
                    ctx.writeAndFlush(Unpooled.wrappedBuffer(new byte[] { 1 }));
                  }
                }
                ctx.fireUserEventTriggered(evt);
              }
            });
          }
        })
        .bind(new LocalAddress(TlsHandshakeMetricsHandlerTest.class.getSimpleName()))
        .sync()
        .channel();

    return serverChildChannel;
  }

  /// Connects, attempts a handshake (or fails) a handshake, waits for the server's first byte on success, then disconnects
  private void connect(@Nullable final String[] signatureSchemes) throws Exception {
    serverHandshakeFuture = new CompletableFuture<>();
    final CompletableFuture<Void> clientDone = new CompletableFuture<>();

    final Channel clientChannel = new Bootstrap()
        .group(eventLoopGroup)
        .channel(LocalChannel.class)
        .handler(new ChannelInitializer<LocalChannel>() {
          @Override
          protected void initChannel(final LocalChannel ch) {
            final SSLEngine engine = clientSslContext.newEngine(ch.alloc(), DOMAIN, 443);

            final SSLParameters parameters = engine.getSSLParameters();
            parameters.setServerNames(List.of(new SNIHostName(DOMAIN)));
            if (signatureSchemes != null && signatureSchemes.length > 0) {
              parameters.setSignatureSchemes(signatureSchemes);
            }
            engine.setSSLParameters(parameters);

            ch.pipeline().addLast(new SslHandler(engine));
            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
                ((ByteBuf) msg).release();
                clientDone.complete(null);
              }

              @Override
              public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
                if (evt instanceof SslHandshakeCompletionEvent handshakeCompletionEvent
                    && !handshakeCompletionEvent.isSuccess()) {
                  clientDone.complete(null);
                }
              }

              @Override
              public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
                clientDone.complete(null);
              }
            });
          }
        })
        .connect(serverChannel.localAddress())
        .sync()
        .channel();

    try {
      CompletableFuture.allOf(clientDone, serverHandshakeFuture)
          .get(5, TimeUnit.SECONDS);
    } finally {
      clientChannel.close().sync();
    }
  }

  /// @return the counter value where the “success” tag’s value equals `success`
  private double getCount(final boolean success) {
    @Nullable final Counter counter = meterRegistry.find(TlsHandshakeMetricsHandler.HANDSHAKE_COUNTER_NAME)
        .tag(TlsHandshakeMetricsHandler.SUCCESS_TAG_NAME, String.valueOf(success))
        .counter();

    return counter != null ? counter.count() : 0;
  }

  private double getTotalCount() {
    return meterRegistry.find(TlsHandshakeMetricsHandler.HANDSHAKE_COUNTER_NAME).counters().stream()
        .mapToDouble(Counter::count)
        .sum();
  }
}
