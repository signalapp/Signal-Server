/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;
import io.netty.util.Mapping;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class SniMapperTest {

  // Configuration for precomputed keystore blob defined in sni-mapper-test-keystore.p12
  private static final String FOO_DOMAIN = "foo.example.com";
  private static final String BAR_DOMAIN = "bar.example.com";

  private static final CertificateBuilder.Algorithm[] ALGORITHMS = new CertificateBuilder.Algorithm[] {
      CertificateBuilder.Algorithm.ed25519,
      CertificateBuilder.Algorithm.rsa2048
  };

  private static DefaultEventLoopGroup eventLoopGroup;
  private static final Map<SslProvider, Mapping<String, SslContext>> sniMappingsByProvider =
      new EnumMap<>(SslProvider.class);

  private Channel serverChannel;

  @BeforeAll
  static void setUpBeforeAll() throws Exception {
    final Instant now = Instant.now();

    final KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null);

    final char[] keyStorePassword = RandomStringUtils.insecure().nextAlphanumeric(16).toCharArray();

    for (final String domain : new String[] { FOO_DOMAIN, BAR_DOMAIN }) {
      for (final CertificateBuilder.Algorithm algorithm : ALGORITHMS) {
        final X509Bundle x509Bundle = new CertificateBuilder()
            .notBefore(now)
            .notAfter(now.plus(Duration.ofDays(1)))
            .setIsCertificateAuthority(true)
            .algorithm(algorithm)
            .subject("CN=" + domain)
            .addSanDnsName(domain)
            .buildSelfSigned();

        keyStore.setEntry(domain + "-" + algorithm,
            new KeyStore.PrivateKeyEntry(x509Bundle.getKeyPair().getPrivate(), x509Bundle.getCertificatePath()),
            new KeyStore.PasswordProtection(keyStorePassword));
      }
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    keyStore.store(byteArrayOutputStream, keyStorePassword);

    for (final SslProvider sslProvider : new SslProvider[] { SslProvider.JDK, SslProvider.OPENSSL }) {
      sniMappingsByProvider.put(sslProvider, SniMapper.buildSniMapping(
          new ByteArrayInputStream(byteArrayOutputStream.toByteArray()), new String(keyStorePassword), sslProvider));
    }

    eventLoopGroup = new DefaultEventLoopGroup();
  }

  private void startServer(final SslProvider sslProvider) throws InterruptedException {
    final Mapping<String, SslContext> sniMapping = sniMappingsByProvider.get(sslProvider);
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
  }

  @AfterAll
  static void tearDownAfterAll() throws InterruptedException {
    eventLoopGroup.shutdownGracefully(1, 1000, TimeUnit.MILLISECONDS).sync();
  }

  @ParameterizedTest
  @EnumSource(value = SslProvider.class, names = {"JDK", "OPENSSL"})
  void unknownDomain(final SslProvider sslProvider) throws Exception {
    assertNotNull(sniMappingsByProvider.get(sslProvider).map("unknown.example.com"));
    startServer(sslProvider);
    final X509Certificate defaultCertificate = connectAndGetServerCertificate("unknown.example.com", null);

    // bar.example.com is the lexicographically first domain, so we should default to it.
    assertCertificateIsForDomain(defaultCertificate, BAR_DOMAIN);
  }

  static List<Arguments> selectCertificate() {
    final List<Arguments> cases = List.of(
        Arguments.of(FOO_DOMAIN, List.of(), "Ed25519"),
        Arguments.of(BAR_DOMAIN, List.of(), "Ed25519"),
        Arguments.of(BAR_DOMAIN, List.of("ed25519"), "Ed25519"),
        Arguments.of(FOO_DOMAIN, List.of("rsa_pss_rsae_sha256", "rsa_pss_rsae_sha384", "rsa_pss_rsae_sha512", "rsa_pkcs1_sha256"), "SHA256withRSA"),
        Arguments.of(FOO_DOMAIN, List.of("rsa_pss_rsae_sha256", "rsa_pss_rsae_sha384", "rsa_pss_rsae_sha512", "rsa_pkcs1_sha256", "ed25519"), "SHA256withRSA"),
        Arguments.of(FOO_DOMAIN, List.of("ed25519", "rsa_pss_rsae_sha256", "rsa_pss_rsae_sha384", "rsa_pss_rsae_sha512"), "Ed25519")
    );

    // Each provider should select the same certificate
    return Stream.of(SslProvider.JDK, SslProvider.OPENSSL)
        .flatMap(sslProvider -> cases.stream().map(arguments -> {
          final Object[] args = arguments.get();
          return Arguments.of(sslProvider, args[0], args[1], args[2]);
        }))
        .toList();
  }

  @ParameterizedTest
  @MethodSource
  void selectCertificate(final SslProvider sslProvider, final String sni, final List<String> signatureSchemes,
      final String expectedSigAlgorithm) throws Exception {
    startServer(sslProvider);
    final X509Certificate serverCert = connectAndGetServerCertificate(sni, signatureSchemes.toArray(String[]::new));
    assertNotNull(serverCert);
    assertCertificateIsForDomain(serverCert, sni);
    assertEquals(expectedSigAlgorithm, serverCert.getSigAlgName());
  }

  @ParameterizedTest
  @EnumSource(value = SslProvider.class, names = {"JDK", "OPENSSL"})
  void noCommonSignatureAlgorithm(final SslProvider sslProvider) throws Exception {
    startServer(sslProvider);

    final ExecutionException executionException = assertThrows(ExecutionException.class,
        () -> connectAndGetServerCertificate(FOO_DOMAIN, new String[] { "ecdsa_secp256r1_sha256" }),
        "server doesn’t have an ECDSA key");

    assertInstanceOf(SSLException.class, executionException.getCause());
  }

  private X509Certificate connectAndGetServerCertificate(final String sniHostname,
      final String[] signatureSchemes) throws Exception {
    final SslContext clientSsl = SslContextBuilder.forClient()
        // the client can always use the JDK provider
        .sslProvider(SslProvider.JDK)
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
