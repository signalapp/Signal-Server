package org.whispersystems.textsecuregcm.recaptcha;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

public class RecaptchaClient {

  private final Client client;
  private final String recaptchaSecret;

  public RecaptchaClient(String recaptchaSecret) {
    this.client          = ClientBuilder.newClient(new ClientConfig(new JacksonJaxbJsonProvider().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)));
    this.recaptchaSecret = recaptchaSecret;
  }

  public boolean verify(String captchaToken) {
    MultivaluedMap<String, String> formData = new MultivaluedHashMap<>();
    formData.add("secret", recaptchaSecret);
    formData.add("response", captchaToken);

    VerifyResponse response = client.target("https://www.google.com/recaptcha/api/siteverify")
                                    .request()
                                    .post(Entity.form(formData), VerifyResponse.class);

    return response.success;
  }

  private static class VerifyResponse {
    @JsonProperty
    private boolean success;

    @JsonProperty("error-codes")
    private String[] errorCodes;

    @JsonProperty
    private String hostname;

    @JsonProperty
    private String challenge_ts;

    @Override
    public String toString() {
      return "success: " + success + ", errorCodes: " + String.join(", ", errorCodes == null ? new String[0] : errorCodes) + ", hostname: " + hostname + ", challenge_ts: " + challenge_ts;
    }
  }

}

