/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.bouncycastle.openssl.PEMReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class CertificateUtil {
    public static KeyStore buildKeyStoreForPem(final String caCertificatePem) throws CertificateException
    {
        try {
            X509Certificate certificate = getCertificate(caCertificatePem);

            if (certificate == null) {
                throw new CertificateException("No certificate found in parsing!");
            }

            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null);
            keyStore.setCertificateEntry("ca", certificate);
            return keyStore;
        } catch (IOException | KeyStoreException ex) {
            throw new CertificateException(ex);
        } catch (NoSuchAlgorithmException ex) {
            throw new AssertionError(ex);
        }
    }

    public static X509Certificate getCertificate(final String certificatePem) throws CertificateException {
        try (PEMReader reader = new PEMReader(new InputStreamReader(new ByteArrayInputStream(certificatePem.getBytes())))) {
            return (X509Certificate) reader.readObject();
        } catch (IOException e) {
            throw new CertificateException(e);
        }
    }
}
