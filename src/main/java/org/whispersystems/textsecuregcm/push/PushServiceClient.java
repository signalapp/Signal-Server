package org.whispersystems.textsecuregcm.push;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.PushConfiguration;
import org.whispersystems.textsecuregcm.entities.ApnMessage;
import org.whispersystems.textsecuregcm.entities.GcmMessage;
import org.whispersystems.textsecuregcm.util.Base64;

import javax.ws.rs.core.MediaType;

public class PushServiceClient {

  private static final String PUSH_GCM_PATH = "/api/v1/push/gcm";
  private static final String PUSH_APN_PATH = "/api/v1/push/apn";

  private final Logger logger = LoggerFactory.getLogger(PushServiceClient.class);

  private final Client client;
  private final String host;
  private final int    port;
  private final String authorization;

  public PushServiceClient(Client client, PushConfiguration config) {
    this.client        = client;
    this.host          = config.getHost();
    this.port          = config.getPort();
    this.authorization = getAuthorizationHeader(config.getUsername(), config.getPassword());
  }

  public void send(GcmMessage message) throws TransientPushFailureException {
    sendPush(PUSH_GCM_PATH, message);
  }

  public void send(ApnMessage message) throws TransientPushFailureException {
    sendPush(PUSH_APN_PATH, message);
  }

  private void sendPush(String path, Object entity) throws TransientPushFailureException {
    ClientResponse response = client.resource("http://" + host + ":" + port + path)
                                    .header("Authorization", authorization)
                                    .entity(entity, MediaType.APPLICATION_JSON)
                                    .put(ClientResponse.class);

    if (response.getStatus() != 204 && response.getStatus() != 200) {
      logger.warn("PushServer response: " + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
      throw new TransientPushFailureException("Bad response: " + response.getStatus());
    }
  }

  private String getAuthorizationHeader(String username, String password) {
    return "Basic " + Base64.encodeBytes((username + ":" + password).getBytes());
  }
}
