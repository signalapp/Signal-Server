/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import com.google.common.annotations.VisibleForTesting;
import io.netty.handler.ssl.OpenSsl;
import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Optional;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

/// A server key manager for netty's BoringSSL provider that uses a client's signature algorithm preferences, enabling
/// support for ed25519.
///
/// This works around [netty#17690](https://github.com/netty/netty/issues/17690):
/// netty’s engine only asks the key manager for RSA- and EC-family key types, so it never selects an Ed25519 key.
class BoringSslServerKeyManager extends X509ExtendedKeyManager {

  private final X509ExtendedKeyManager delegate;

  @VisibleForTesting
  BoringSslServerKeyManager(final X509ExtendedKeyManager delegate) {
    if (!"BoringSSL".equals(OpenSsl.versionString())) {
      throw new IllegalStateException("BoringSSL required, found " + OpenSsl.versionString());
    }
    this.delegate = delegate;
  }

  /// @throws IllegalArgumentException if `keyManagers` contains no [X509ExtendedKeyManager]
  static BoringSslServerKeyManager wrap(final KeyManager[] keyManagers) {
    return Arrays.stream(keyManagers)
        .filter(X509ExtendedKeyManager.class::isInstance)
        .map(X509ExtendedKeyManager.class::cast)
        .findFirst()
        .map(BoringSslServerKeyManager::new)
        .orElseThrow(() -> new IllegalArgumentException("No X509ExtendedKeyManager found"));
  }

  @Override
  public String chooseEngineServerAlias(final String keyType, final Principal[] issuers, final SSLEngine engine) {
    if (!(engine.getHandshakeSession() instanceof ExtendedSSLSession session)) {
      return null;
    }

    // Netty will only call this with EC/RSA key types (https://github.com/netty/netty/issues/17690).
    // We can safely ignore `keyType` and loop through the peer's signature algorithms to find the best matching key.
    for (final String signatureAlgorithm : session.getPeerSupportedSignatureAlgorithms()) {
      final Optional<String> maybeAlias = getJdkKeyType(signatureAlgorithm)
          .map(jdkKeyType -> delegate.chooseEngineServerAlias(jdkKeyType, issuers, engine));

      if (maybeAlias.isPresent()) {
        return maybeAlias.get();
      }
    }

    return null;
  }

  /// Maps a JDK signature algorithm name to the JDK key type that can produce it
  @VisibleForTesting
  static Optional<String> getJdkKeyType(final String signatureAlgorithm) {
    if (signatureAlgorithm.equals("Ed25519")) {
      return Optional.of("EdDSA");
    } else if (signatureAlgorithm.endsWith("withECDSA")) {
      return Optional.of("EC");
    } else if (signatureAlgorithm.endsWith("withRSA")) {
      return Optional.of("RSA");
    }

    // We don't need to be exhaustive, because we require TLS 1.3 and only use a limited number of key types.
    return Optional.empty();
  }

  @Override
  public String chooseServerAlias(final String keyType, final Principal[] issuers, final Socket socket) {
    return delegate.chooseServerAlias(keyType, issuers, socket);
  }

  @Override
  public String[] getServerAliases(final String keyType, final Principal[] issuers) {
    return delegate.getServerAliases(keyType, issuers);
  }

  @Override
  public String chooseClientAlias(final String[] keyTypes, final Principal[] issuers, final Socket socket) {
    return delegate.chooseClientAlias(keyTypes, issuers, socket);
  }

  @Override
  public String[] getClientAliases(final String keyType, final Principal[] issuers) {
    return delegate.getClientAliases(keyType, issuers);
  }

  @Override
  public X509Certificate[] getCertificateChain(final String alias) {
    return delegate.getCertificateChain(alias);
  }

  @Override
  public PrivateKey getPrivateKey(final String alias) {
    return delegate.getPrivateKey(alias);
  }
}
