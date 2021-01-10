/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.SharedMetricRegistries;
import org.bouncycastle.openssl.PEMReader;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.whispersystems.textsecuregcm.configuration.DirectoryServerConfiguration;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse;
import org.whispersystems.textsecuregcm.util.CertificateExpirationGauge;
import org.whispersystems.textsecuregcm.util.Constants;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static com.codahale.metrics.MetricRegistry.name;

public class DirectoryReconciliationClient {

  private final String replicationUrl;
  private final Client client;

  public DirectoryReconciliationClient(DirectoryServerConfiguration directoryServerConfiguration)
      throws CertificateException
  {
    this.replicationUrl = directoryServerConfiguration.getReplicationUrl();
    this.client         = initializeClient(directoryServerConfiguration);

    SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME)
                          .register(name(getClass(), directoryServerConfiguration.getReplicationName(), "days_until_certificate_expiration"),
                                    new CertificateExpirationGauge(getCertificate(directoryServerConfiguration.getReplicationCaCertificate())));
  }

  public DirectoryReconciliationResponse sendChunk(DirectoryReconciliationRequest request) {
    return client.target(replicationUrl)
                 .path("/v2/directory/reconcile")
                 .request(MediaType.APPLICATION_JSON_TYPE)
                 .put(Entity.json(request), DirectoryReconciliationResponse.class);
  }

  private static Client initializeClient(DirectoryServerConfiguration directoryServerConfiguration)
      throws CertificateException
  {
    KeyStore   trustStore = initializeKeyStore(directoryServerConfiguration.getReplicationCaCertificate());
    SSLContext sslContext = SslConfigurator.newInstance()
                                           .securityProtocol("TLSv1.2")
                                           .trustStore(trustStore)
                                           .createSSLContext();
    return ClientBuilder.newBuilder()
                        .register(HttpAuthenticationFeature.basic("signal", directoryServerConfiguration.getReplicationPassword().getBytes()))
                        .sslContext(sslContext)
                        .build();
  }

  private static KeyStore initializeKeyStore(String caCertificatePem)
      throws CertificateException
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

  private static X509Certificate getCertificate(final String certificatePem) throws CertificateException {
    try (PEMReader reader = new PEMReader(new InputStreamReader(new ByteArrayInputStream(certificatePem.getBytes())))) {
      return (X509Certificate) reader.readObject();
    } catch (IOException e) {
      throw new CertificateException(e);
    }
  }
}
