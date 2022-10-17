/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class CertificateUtil {

  public static KeyStore buildKeyStoreForPem(final String... caCertificatePems) throws CertificateException {
    try {
      final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null);

      for (int i = 0; i < caCertificatePems.length; i++) {
        final X509Certificate certificate = getCertificate(caCertificatePems[i]);

        if (certificate == null) {
          throw new CertificateException("No certificate found in parsing!");
        }

        keyStore.setCertificateEntry("ca-" + i, certificate);
      }

      return keyStore;
    } catch (IOException | KeyStoreException ex) {
      throw new CertificateException(ex);
    } catch (NoSuchAlgorithmException ex) {
      throw new AssertionError(ex);
    }
  }

  public static X509Certificate getCertificate(final String certificatePem) throws CertificateException {
    final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");

    try (final ByteArrayInputStream pemInputStream = new ByteArrayInputStream(certificatePem.getBytes())) {
      return (X509Certificate) certificateFactory.generateCertificate(pemInputStream);
    } catch (IOException e) {
      throw new CertificateException(e);
    }
  }
}
