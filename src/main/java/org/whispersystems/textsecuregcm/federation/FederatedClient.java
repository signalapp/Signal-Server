/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.federation;


import com.google.common.base.Optional;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.bouncycastle.openssl.PEMReader;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.AccountCount;
import org.whispersystems.textsecuregcm.entities.AttachmentUri;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.entities.ClientContacts;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.PreKeyResponse;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.setup.Environment;

public class FederatedClient {

  private final Logger logger = LoggerFactory.getLogger(FederatedClient.class);

  private static final String USER_COUNT_PATH       = "/v1/federation/user_count";
  private static final String USER_TOKENS_PATH      = "/v1/federation/user_tokens/%d";
  private static final String RELAY_MESSAGE_PATH    = "/v1/federation/messages/%s/%d/%s";
  private static final String PREKEY_PATH_DEVICE_V1 = "/v1/federation/key/%s/%s";
  private static final String PREKEY_PATH_DEVICE_V2 = "/v2/federation/key/%s/%s";
  private static final String ATTACHMENT_URI_PATH   = "/v1/federation/attachment/%d";
  private static final String RECEIPT_PATH          = "/v1/receipt/%s/%d/%s/%d";

  private final FederatedPeer peer;
  private final Client        client;

  public FederatedClient(Environment environment, JerseyClientConfiguration configuration,
                         String federationName, FederatedPeer peer)
      throws IOException
  {
    try {
      this.client = createClient(environment, configuration, federationName, peer);
      this.peer   = peer;
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    } catch (KeyStoreException | KeyManagementException | CertificateException e) {
      throw new IOException(e);
    }
  }

  public URL getSignedAttachmentUri(long attachmentId) throws IOException {
    try {
      AttachmentUri response = client.target(peer.getUrl())
                                     .path(String.format(ATTACHMENT_URI_PATH, attachmentId))
                                     .request()
                                     .accept(MediaType.APPLICATION_JSON_TYPE)
                                     .get(AttachmentUri.class);

      return response.getLocation();
    } catch (ProcessingException e) {
      logger.warn("Bad URI", e);
      throw new IOException(e);
    }
  }

  public Optional<PreKeyResponse> getKeysV2(String destination, String device) {
    try {
      PreKeyResponse response = client.target(peer.getUrl())
                                      .path(String.format(PREKEY_PATH_DEVICE_V2, destination, device))
                                      .request()
                                      .accept(MediaType.APPLICATION_JSON_TYPE)
                                      .get(PreKeyResponse.class);

      return Optional.of(response);
    } catch (ProcessingException e) {
      logger.warn("PreKey", e);
      return Optional.absent();
    }
  }

  public int getUserCount() {
    try {
      AccountCount count = client.target(peer.getUrl())
                                 .path(USER_COUNT_PATH)
                                 .request()
                                 .accept(MediaType.APPLICATION_JSON_TYPE)
                                 .get(AccountCount.class);

      return count.getCount();
    } catch (ProcessingException e) {
      logger.warn("User Count", e);
      return 0;
    }
  }

  public List<ClientContact> getUserTokens(int offset) {
    try {
      ClientContacts contacts = client.target(peer.getUrl())
                                      .path(String.format(USER_TOKENS_PATH, offset))
                                      .request()
                                      .accept(MediaType.APPLICATION_JSON_TYPE)
                                      .get(ClientContacts.class);

      return contacts.getContacts();
    } catch (ProcessingException e) {
      logger.warn("User Tokens", e);
      return null;
    }
  }

  public void sendMessages(String source, long sourceDeviceId, String destination, IncomingMessageList messages)
      throws IOException
  {
    Response response = null;

    try {
      response = client.target(peer.getUrl())
                       .path(String.format(RELAY_MESSAGE_PATH, source, sourceDeviceId, destination))
                       .request()
                       .put(Entity.json(messages));

      if (response.getStatus() != 200 && response.getStatus() != 204) {
        if (response.getStatus() == 411) throw new WebApplicationException(Response.status(413).build());
        else                             throw new WebApplicationException(Response.status(response.getStatusInfo()).build());
      }

    } catch (ProcessingException e) {
      logger.warn("sendMessage", e);
      throw new IOException(e);
    } finally {
      if (response != null) response.close();
    }
  }

  public void sendDeliveryReceipt(String source, long sourceDeviceId, String destination, long messageId)
      throws IOException
  {
    Response response = null;

    try {
      response = client.target(peer.getUrl())
                       .path(String.format(RECEIPT_PATH, source, sourceDeviceId, destination, messageId))
                       .request()
                       .property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true)
                       .put(Entity.entity("", MediaType.APPLICATION_JSON_TYPE));

      if (response.getStatus() != 200 && response.getStatus() != 204) {
        if (response.getStatus() == 411) throw new WebApplicationException(Response.status(413).build());
        else                             throw new WebApplicationException(Response.status(response.getStatusInfo()).build());
      }
    } catch (ProcessingException e) {
      logger.warn("sendMessage", e);
      throw new IOException(e);
    } finally {
      if (response != null) response.close();
    }
  }

  private Client createClient(Environment environment, JerseyClientConfiguration configuration,
                              String federationName, FederatedPeer peer)
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, CertificateException
  {
    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
    trustManagerFactory.init(initializeTrustStore(peer.getName(), peer.getCertificate()));

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, trustManagerFactory.getTrustManagers(), new SecureRandom());

    SSLConnectionSocketFactory        sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslContext, new DefaultHostnameVerifier());
    Registry<ConnectionSocketFactory> registry                   = RegistryBuilder.<ConnectionSocketFactory>create().register("https", sslConnectionSocketFactory).build();

    Client client = new JerseyClientBuilder(environment).using(configuration)
                                                        .using(registry)
                                                        .build("FederatedClient");

    client.property(ClientProperties.CONNECT_TIMEOUT, 5000);
    client.property(ClientProperties.READ_TIMEOUT, 10000);
    client.property(ClientProperties.REQUEST_ENTITY_PROCESSING, RequestEntityProcessing.BUFFERED);
    client.register(HttpAuthenticationFeature.basic(federationName, peer.getAuthenticationToken()));

    return client;
  }

  private KeyStore initializeTrustStore(String name, String pemCertificate)
      throws CertificateException
  {
    try {
      PEMReader       reader      = new PEMReader(new InputStreamReader(new ByteArrayInputStream(pemCertificate.getBytes())));
      X509Certificate certificate = (X509Certificate) reader.readObject();

      if (certificate == null) {
        throw new CertificateException("No certificate found in parsing!");
      }

      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null);
      keyStore.setCertificateEntry(name, certificate);

      return keyStore;
    } catch (IOException | KeyStoreException e) {
      throw new CertificateException(e);
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  public String getPeerName() {
    return peer.getName();
  }
}
