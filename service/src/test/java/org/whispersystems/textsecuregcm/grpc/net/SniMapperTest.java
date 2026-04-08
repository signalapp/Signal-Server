/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.SimpleUserEventChannelHandler;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.ssl.SniHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.Mapping;
import java.io.InputStream;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SniMapperTest {

  // Configuration for precomputed keystore blob defined in sni-mapper-test-keystore.p12
  private static final String FOO_DOMAIN = "foo.example.com";
  private static final String BAR_DOMAIN = "bar.example.com";
  private static final String KEY_STORE_PASSWORD = "password";
  private static final String KEY_STORE_NAME = "sni-mapper-test-keystore.p12";

  private DefaultEventLoopGroup eventLoopGroup;
  private Channel serverChannel;

  @BeforeEach
  void setUp() throws Exception {
    final InputStream keyStore = SniMapper.class.getResourceAsStream(KEY_STORE_NAME);
    eventLoopGroup = new DefaultEventLoopGroup();

    final Mapping<String, SslContext> sniMapping =
        SniMapper.buildSniMapping(keyStore, KEY_STORE_PASSWORD);

    final LocalAddress localAddress = new LocalAddress(SniMapper.class.getSimpleName());
    serverChannel = new ServerBootstrap()
        .group(eventLoopGroup)
        .channel(LocalServerChannel.class)
        .childHandler(new ChannelInitializer<>() {
          @Override
          protected void initChannel(final Channel ch) {
            ch.pipeline().addLast(new SniHandler(sniMapping));
          }
        })
        .bind(localAddress)
        .sync()
        .channel();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (serverChannel != null) {
      serverChannel.close().sync();
    }
    eventLoopGroup.shutdownGracefully(1, 1000, TimeUnit.MILLISECONDS).sync();
  }

  @Test
  void unknownDomain() throws Exception {
    final InputStream keyStore = SniMapper.class.getResourceAsStream(KEY_STORE_NAME);
    final Mapping<String, SslContext> sniMapping = SniMapper.buildSniMapping(keyStore, KEY_STORE_PASSWORD);
    assertNotNull(sniMapping.map("unknown.example.com"));
    final X509Certificate defaultCertificate = connectAndGetServerCertificate("unknown.example.com", null);

    // bar.example.com is the lexicographically first domain, so we should default to it.
    assertCertificateIsForDomain(defaultCertificate, BAR_DOMAIN);
  }

  static List<Arguments> selectCertificate() {
    return List.of(
        Arguments.of(FOO_DOMAIN, List.of(), "Ed25519"),
        Arguments.of(BAR_DOMAIN, List.of(), "Ed25519"),
        Arguments.of(BAR_DOMAIN, List.of("ed25519"), "Ed25519"),
        Arguments.of(FOO_DOMAIN, List.of("rsa_pss_rsae_sha256", "rsa_pss_rsae_sha384", "rsa_pss_rsae_sha512"), "SHA256withRSA"),
        Arguments.of(FOO_DOMAIN, List.of("rsa_pss_rsae_sha256", "rsa_pss_rsae_sha384", "rsa_pss_rsae_sha512", "ed25519"), "SHA256withRSA"),
        Arguments.of(FOO_DOMAIN, List.of("ed25519", "rsa_pss_rsae_sha256", "rsa_pss_rsae_sha384", "rsa_pss_rsae_sha512"), "Ed25519")
    );
  }

  @ParameterizedTest
  @MethodSource
  void selectCertificate(final String sni, final List<String> signatureSchemes, final String expectedSigAlgorithm)
      throws Exception {
    final X509Certificate serverCert = connectAndGetServerCertificate(sni, signatureSchemes.toArray(String[]::new));
    assertNotNull(serverCert);
    assertCertificateIsForDomain(serverCert, sni);
    assertEquals(expectedSigAlgorithm, serverCert.getSigAlgName());
  }

  private X509Certificate connectAndGetServerCertificate(final String sniHostname,
      final String[] signatureSchemes) throws Exception {
    final SslContext clientSsl = SslContextBuilder.forClient()
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .protocols("TLSv1.3")
        .build();

    final CompletableFuture<X509Certificate> certFuture = new CompletableFuture<>();

    final Bootstrap clientBootstrap = new Bootstrap()
        .group(eventLoopGroup)
        .channel(LocalChannel.class)
        .handler(new ChannelInitializer<LocalChannel>() {
          @Override
          protected void initChannel(final LocalChannel ch) {
            final SSLEngine engine = clientSsl.newEngine(ch.alloc());

            final SSLParameters params = engine.getSSLParameters();
            params.setServerNames(List.of(new SNIHostName(sniHostname)));
            if (signatureSchemes != null && signatureSchemes.length != 0) {
              params.setSignatureSchemes(signatureSchemes);
            }
            engine.setSSLParameters(params);

            final SslHandler sslHandler = new SslHandler(engine);
            ch.pipeline().addLast(sslHandler);
            ch.pipeline().addLast(new SimpleUserEventChannelHandler<SslHandshakeCompletionEvent>() {
              @Override
              protected void eventReceived(final ChannelHandlerContext ctx, final SslHandshakeCompletionEvent evt) {
                if (!evt.isSuccess()) {
                  certFuture.completeExceptionally(evt.cause());
                  return;
                }
                try {
                  final SSLSession session = sslHandler.engine().getSession();
                  final X509Certificate cert = (X509Certificate) session.getPeerCertificates()[0];
                  certFuture.complete(cert);
                } catch (final SSLPeerUnverifiedException e) {
                  certFuture.completeExceptionally(e);
                }
              }
            });
          }
        });

    final Channel clientChannel = clientBootstrap.connect(serverChannel.localAddress()).sync().channel();
    try {
      return certFuture.get(5, TimeUnit.SECONDS);
    } finally {
      clientChannel.close().sync();
    }
  }

  private static void assertCertificateIsForDomain(final X509Certificate cert, final String expectedDomain)
      throws Exception {
    assertTrue(cert.getSubjectAlternativeNames().stream()
        .filter(san -> (int) san.getFirst() == 2) // dNSName
        .map(san -> (String) san.get(1))
        .anyMatch(name -> name.equalsIgnoreCase(expectedDomain)));
  }
}
