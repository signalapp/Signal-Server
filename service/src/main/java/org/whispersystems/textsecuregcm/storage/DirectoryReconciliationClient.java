/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

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
import org.whispersystems.textsecuregcm.util.CertificateUtil;

public class DirectoryReconciliationClient {

  private final String replicationUrl;
  private final Client client;

  public DirectoryReconciliationClient(DirectoryServerConfiguration directoryServerConfiguration)
      throws CertificateException
  {
    this.replicationUrl = directoryServerConfiguration.getReplicationUrl();
    this.client = initializeClient(directoryServerConfiguration);
  }

  public DirectoryReconciliationResponse add(DirectoryReconciliationRequest request) {
    return client.target(replicationUrl)
        .path("/v3/directory/exists")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.json(request), DirectoryReconciliationResponse.class);
  }

  public DirectoryReconciliationResponse delete(DirectoryReconciliationRequest request) {
    return client.target(replicationUrl)
        .path("/v3/directory/deletes")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.json(request), DirectoryReconciliationResponse.class);
  }

  public DirectoryReconciliationResponse complete() {
    return client.target(replicationUrl)
        .path("/v3/directory/complete")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(null, DirectoryReconciliationResponse.class);
  }

  private static Client initializeClient(DirectoryServerConfiguration directoryServerConfiguration)
      throws CertificateException {
    KeyStore trustStore = CertificateUtil.buildKeyStoreForPem(
        directoryServerConfiguration.getReplicationCaCertificates().toArray(new String[0]));
    SSLContext sslContext = SslConfigurator.newInstance()
        .securityProtocol("TLSv1.2")
        .trustStore(trustStore)
        .createSSLContext();

    return ClientBuilder.newBuilder()
        .register(
            HttpAuthenticationFeature.basic("signal", directoryServerConfiguration.getReplicationPassword().getBytes()))
        .sslContext(sslContext)
        .build();
  }
}
