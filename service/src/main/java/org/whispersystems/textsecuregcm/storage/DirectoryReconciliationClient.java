/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.SharedMetricRegistries;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.whispersystems.textsecuregcm.configuration.DirectoryServerConfiguration;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse;
import org.whispersystems.textsecuregcm.util.CertificateExpirationGauge;
import org.whispersystems.textsecuregcm.util.CertificateUtil;
import org.whispersystems.textsecuregcm.util.Constants;

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
                                    new CertificateExpirationGauge(CertificateUtil.getCertificate(directoryServerConfiguration.getReplicationCaCertificate())));
  }

  public DirectoryReconciliationResponse sendChunk(DirectoryReconciliationRequest request) {
    return client.target(replicationUrl)
                 .path("/v2/directory/reconcile")
                 .request(MediaType.APPLICATION_JSON_TYPE)
                 .put(Entity.json(request), DirectoryReconciliationResponse.class);
  }

  public DirectoryReconciliationResponse delete(DirectoryReconciliationRequest request) {
    return client.target(replicationUrl)
        .path("/v3/directory/deletes")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.json(request), DirectoryReconciliationResponse.class);
  }

  private static Client initializeClient(DirectoryServerConfiguration directoryServerConfiguration)
      throws CertificateException
  {
    KeyStore   trustStore = CertificateUtil.buildKeyStoreForPem(directoryServerConfiguration.getReplicationCaCertificate());
    SSLContext sslContext = SslConfigurator.newInstance()
                                           .securityProtocol("TLSv1.2")
                                           .trustStore(trustStore)
                                           .createSSLContext();
    return ClientBuilder.newBuilder()
                        .register(HttpAuthenticationFeature.basic("signal", directoryServerConfiguration.getReplicationPassword().getBytes()))
                        .sslContext(sslContext)
                        .build();
  }
}
