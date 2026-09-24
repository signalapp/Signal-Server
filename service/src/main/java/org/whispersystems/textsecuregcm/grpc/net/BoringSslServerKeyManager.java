/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import com.google.common.annotations.VisibleForTesting;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.ReferenceCountedOpenSslEngine;
import io.netty.internal.tcnative.SSL;
import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509KeyManager;

/// A server key manager for netty's BoringSSL provider that uses a client's signature algorithm preferences, enabling
/// support for ed25519.
///
/// This works around [netty#10916](https://github.com/netty/netty/issues/10916) by addressing two issues in key selection:
///
///    1. Netty’s engine only declares support for RSA- and EC-family key types, and
///    2. the handshake session's peer signature algorithm converter (`io.netty.handler.ssl.SignatureAlgorithmConverter`)
///       doesn't match BoringSSL's `ed25519`, so ed25519 gets dropped, even if one is presented as a candidate
class BoringSslServerKeyManager extends X509ExtendedKeyManager {

  private final X509KeyManager delegate;

  @VisibleForTesting
  BoringSslServerKeyManager(final X509KeyManager delegate) {
    if (!"BoringSSL".equals(OpenSsl.versionString())) {
      throw new IllegalStateException("BoringSSL required, found " + OpenSsl.versionString());
    }
    this.delegate = delegate;
  }

  /// @throws IllegalArgumentException if `keyManagers` contains no [X509KeyManager]
  static BoringSslServerKeyManager wrap(final KeyManager[] keyManagers) {
    return Arrays.stream(keyManagers)
        .filter(X509KeyManager.class::isInstance)
        .map(X509KeyManager.class::cast)
        .findFirst()
        .map(BoringSslServerKeyManager::new)
        .orElseThrow(() -> new IllegalArgumentException("No X509KeyManager found"));
  }

  @Override
  public String chooseEngineServerAlias(final String keyType, final Principal[] issuers, final SSLEngine engine) {
    if (!(engine instanceof ReferenceCountedOpenSslEngine openSslEngine)) {
      throw new IllegalArgumentException("BoringSslServerKeyManager requires netty's OpenSSL provider");
    }

    @Nullable final String[] peerSignatureAlgorithms = SSL.getSigAlgs(openSslEngine.sslPointer());
    if (peerSignatureAlgorithms == null) {
      return null;
    }

    for (final String signatureAlgorithm : peerSignatureAlgorithms) {
      final Optional<String> maybeAlias = getJdkKeyType(signatureAlgorithm)
          // The Netty OpenSSL engine doesn't advertise support for Ed25519, and so we can't include the engine in the
          // delegate's selection process. Instead, we loop through the peer signature algorithms and find a matching key.
          .map(jdkKeyType -> delegate.chooseServerAlias(jdkKeyType, issuers, null));

      if (maybeAlias.isPresent()) {
        return maybeAlias.get();
      }
    }

    return null;
  }

  /// Maps a [BoringSSL signature algorithm name](https://github.com/google/boringssl/blob/8525ff3/ssl/ssl_privkey.cc#L420-L433)
  /// to the JDK key type that can produce it
  @VisibleForTesting
  static Optional<String> getJdkKeyType(final String signatureAlgorithm) {
    if (signatureAlgorithm.equals("ed25519")) {
      return Optional.of("EdDSA");
    } else if (signatureAlgorithm.startsWith("ecdsa_")) {
      return Optional.of("EC");
    } else if (signatureAlgorithm.startsWith("rsa_pss_rsae_")) {
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
