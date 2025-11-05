/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.security.CertificateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TlsCertificateExpirationUtil {

  private static final Logger logger = LoggerFactory.getLogger(TlsCertificateExpirationUtil.class);

  private static final String CERTIFICATE_EXPIRATION_GAUGE_NAME = name(TlsCertificateExpirationUtil.class,
      "secondsUntilExpiration");

  public static void configureMetrics(final String keyStorePath, final String keyStorePassword, final String keyStoreType, final String keyStoreProvider) {

    final KeyStore keyStore;
    try {
      keyStore = CertificateUtils.getKeyStore(Resource.newResource(keyStorePath), keyStoreType, keyStoreProvider,
          keyStorePassword);

    } catch (Exception e) {
      throw new RuntimeException("Failed to load keystore " + keyStorePath, e);
    }

    getIdentifiersAndExpirations(keyStore, keyStorePassword)
        .forEach((id, expiration) ->
            Gauge.builder(CERTIFICATE_EXPIRATION_GAUGE_NAME, expiration,
                    then -> Duration.between(Instant.now(), then).toSeconds())
                .tags("id", id)
                .strongReference(true)
                .register(Metrics.globalRegistry)
        );
  }

  @VisibleForTesting
  static Map<String, Instant> getIdentifiersAndExpirations(final KeyStore keyStore, final String password) {

    final Map<String, Instant> identifiersAndExpirations = new HashMap<>();

    try {
      for (final Iterator<String> it = keyStore.aliases().asIterator(); it.hasNext(); ) {

        final Certificate certificate = keyStore.getCertificate(it.next());
        if (certificate instanceof X509Certificate x509Certificate) {

          final String name = getName(x509Certificate);
          final String algorithm = x509Certificate.getPublicKey().getAlgorithm();
          final Instant notAfter = Instant.ofEpochMilli(x509Certificate.getNotAfter().getTime());

          final String identifier = name + ":" + algorithm;
          identifiersAndExpirations.put(identifier, notAfter);

        } else {
          logger.warn("Unexpected certificate type: {}", certificate.getClass().getName());
        }
      }
    } catch (final KeyStoreException e) {
      // This should never happen - this exception is thrown if the keystore is not initialized, which
      // CertificateUtils#getKeyStore does.
      throw new RuntimeException("Failed to read keystore", e);
    } catch (final CertificateParsingException e) {
      throw new IllegalArgumentException("Failed to parse certificate", e);
    }

    return identifiersAndExpirations;
  }

  private static String getName(X509Certificate x509Certificate) throws CertificateParsingException {
    return Optional.ofNullable(x509Certificate.getSubjectAlternativeNames())
        .flatMap(sans -> sans.stream().findFirst())
        .map(altNames -> {

          // each list should be a tuple of
          //   alternative name type ID (integer), name
          if (altNames.size() != 2) {
            logger.warn("Unexpected subject alternative name: {}", altNames);
            return null;
          }

          return switch ((Integer) altNames.getFirst()) {
            case 2, 7 -> // dns, ip
                altNames.getLast().toString();
            default -> null;
          };

        })
        .orElse("unknown");
  }

}
