/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import com.google.common.annotations.VisibleForTesting;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.Mapping;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SniMapper {

  private static final Logger logger = LoggerFactory.getLogger(SniMapper.class);

  private SniMapper() {
  }

  /// Build a [Mapping] from a [KeyStore] that maps from domain (via SAN) to [io.netty.handler.ssl.SslContext] that
  /// can be used with an [io.netty.handler.ssl.SniHandler]. The provided keystore may contain multiple certificates
  /// for a single domain, all matching certificates will be included in the corresponding SslContext. The domain for
  /// a certificate is only determined by the SAN and all certificates must have a SAN. The returned [Mapping] returns
  /// an arbitrary set of certificates if none of the certificates in the keystore match a requested domain, as
  /// permitted by RFC-6066.
  ///
  /// @param keyStorePath     The path to the [KeyStore]
  /// @param keyStorePassword The password for the keyStore
  /// @return A [Mapping] that maps domains to the corresponding [SslContext] containing the certificates for that
  /// domain
  public static Mapping<String, SslContext> buildSniMapping(final String keyStorePath, final String keyStorePassword)
      throws IOException {
    try (final FileInputStream fis = new FileInputStream(keyStorePath)) {
      return buildSniMapping(fis, keyStorePassword);
    }
  }

  @VisibleForTesting
  static Mapping<String, SslContext> buildSniMapping(final InputStream keyStore, final String keyStorePassword)
      throws IOException {
    try {
      final Map<String, KeyStore> domainKeyStores = partitionByDomain(keyStore, keyStorePassword.toCharArray());
      final Map<String, SslContext> sslContextsByDomain = new HashMap<>();
      for (final Map.Entry<String, KeyStore> entry : domainKeyStores.entrySet()) {
        final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(entry.getValue(), keyStorePassword.toCharArray());
        sslContextsByDomain.put(entry.getKey(), buildSslContext(kmf));
      }

      // Netty expects the SNI mapping to always return an SslContext. Per RFC-6066 it's valid to continue the handshake
      // on an SNI mismatch, it's the client's responsibility to check the returned certificate's SNI. We sort first so
      // our choice of certificate is deterministic.
      final SslContext defaultSslContext = sslContextsByDomain.entrySet().stream()
          .min(Map.Entry.comparingByKey())
          .orElseThrow(() -> new IllegalArgumentException("Key store contained no certificates"))
          .getValue();

      logger.info("Loaded TLS contexts for domains: {}", sslContextsByDomain.keySet());
      return hostname -> sslContextsByDomain.getOrDefault(hostname, defaultSslContext);
    } catch (NoSuchAlgorithmException | KeyStoreException | CertificateException | UnrecoverableKeyException e) {
      throw new IOException("Failed to load keystore", e);
    }
  }

  private static SslContext buildSslContext(final KeyManagerFactory kmf) throws SSLException {
    return SslContextBuilder.forServer(kmf)
        .applicationProtocolConfig(new ApplicationProtocolConfig(
            ApplicationProtocolConfig.Protocol.ALPN,
            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
            ApplicationProtocolNames.HTTP_2))
        .protocols("TLSv1.3")
        .build();
  }

  private static Map<String, KeyStore> partitionByDomain(final InputStream keystoreStream, final char[] keystorePassword)
      throws KeyStoreException, CertificateException, UnrecoverableKeyException, IOException, NoSuchAlgorithmException {

    final KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(keystoreStream, keystorePassword);

    // Group key entries by the domain(s) in each certificate's SANs
    final Map<String, KeyStore> domainKeyStores = new HashMap<>();
    for (final String alias : Collections.list(keyStore.aliases())) {
      if (!keyStore.isKeyEntry(alias)) {
        continue;
      }

      final Certificate[] chain = keyStore.getCertificateChain(alias);
      if (chain == null || chain.length == 0) {
        continue;
      }

      final X509Certificate leaf = (X509Certificate) chain[0];
      final Key key = keyStore.getKey(alias, keystorePassword);

      for (final String domain : getDnsNames(leaf)) {
        domainKeyStores
            .computeIfAbsent(domain, _ -> newEmptyKeyStore())
            .setKeyEntry(alias, key, keystorePassword, chain);
      }
    }

    if (domainKeyStores.isEmpty()) {
      throw new IOException("Keystore contains no usable key entries with DNS names");
    }
    return domainKeyStores;

  }

  private static KeyStore newEmptyKeyStore() {
    try {
      final KeyStore ks = KeyStore.getInstance("PKCS12");
      ks.load(null, null);
      return ks;
    } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
      // All Java runtime implementations are required to support PKCS12, and we aren't loading anything from disk
      // so an exception here is impossible.
      throw new AssertionError("Failed to create empty keystore", e);
    }
  }

  /// Extract all DNS-type SAN names on this certificate
  private static List<String> getDnsNames(final X509Certificate cert) throws CertificateParsingException, IOException {
    final Collection<List<?>> sans = cert.getSubjectAlternativeNames();
    if (sans == null) {
      throw new IOException("Certificate did not have SAN extension");
    }
    final List<String> dnsSans = sans.stream()
        // GeneralName type 2 = dNSName. See getSubjectAlternativeNames
        .filter(san -> (int) san.getFirst() == 2)
        .map(san -> (String) san.get(1))
        .map(s -> s.toLowerCase(Locale.ROOT))
        .toList();
    if (dnsSans.isEmpty()) {
      throw new IOException("Certificate did not have a DNS SAN entry");
    }
    return dnsSans;
  }

}
