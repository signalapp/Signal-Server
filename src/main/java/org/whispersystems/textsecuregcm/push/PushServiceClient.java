package org.whispersystems.textsecuregcm.push;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.PushConfiguration;
import org.whispersystems.textsecuregcm.entities.ApnMessage;
import org.whispersystems.textsecuregcm.entities.GcmMessage;
import org.whispersystems.textsecuregcm.entities.UnregisteredEvent;
import org.whispersystems.textsecuregcm.entities.UnregisteredEventList;
import org.whispersystems.textsecuregcm.util.Base64;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;

public class PushServiceClient {

  private static final String PUSH_GCM_PATH     = "/api/v1/push/gcm";
  private static final String PUSH_APN_PATH     = "/api/v1/push/apn";

  private static final String APN_FEEDBACK_PATH = "/api/v1/feedback/apn";
  private static final String GCM_FEEDBACK_PATH = "/api/v1/feedback/gcm";

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

  public List<UnregisteredEvent> getGcmFeedback() throws IOException {
    return getFeedback(GCM_FEEDBACK_PATH);
  }

  public List<UnregisteredEvent> getApnFeedback() throws IOException {
    return getFeedback(APN_FEEDBACK_PATH);
  }

  private void sendPush(String path, Object entity) throws TransientPushFailureException {
    try {
      ClientResponse response = client.resource("http://" + host + ":" + port + path)
                                      .header("Authorization", authorization)
                                      .entity(entity, MediaType.APPLICATION_JSON)
                                      .put(ClientResponse.class);

      if (response.getStatus() != 204 && response.getStatus() != 200) {
        logger.warn("PushServer response: " + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
        throw new TransientPushFailureException("Bad response: " + response.getStatus());
      }
    } catch (UniformInterfaceException | ClientHandlerException e) {
      logger.warn("Push error: ", e);
      throw new TransientPushFailureException(e);
    }
  }

  private List<UnregisteredEvent> getFeedback(String path) throws IOException {
    try {
      UnregisteredEventList unregisteredEvents = client.resource("http://" + host + ":" + port + path)
                                                       .header("Authorization", authorization)
                                                       .get(UnregisteredEventList.class);

      return unregisteredEvents.getDevices();
    } catch (UniformInterfaceException | ClientHandlerException e) {
      logger.warn("Request error:", e);
      throw new IOException(e);
    }
  }

  private String getAuthorizationHeader(String username, String password) {
    return "Basic " + Base64.encodeBytes((username + ":" + password).getBytes());
  }
}
