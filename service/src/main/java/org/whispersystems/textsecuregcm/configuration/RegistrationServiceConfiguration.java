package org.whispersystems.textsecuregcm.configuration;

import javax.validation.constraints.NotBlank;

public class RegistrationServiceConfiguration {

  @NotBlank
  private String host;

  private int port = 443;

  @NotBlank
  private String apiKey;

  @NotBlank
  private String registrationCaCertificate;

  public String getHost() {
    return host;
  }

  public void setHost(final String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(final int port) {
    this.port = port;
  }

  public String getApiKey() {
    return apiKey;
  }

  public void setApiKey(final String apiKey) {
    this.apiKey = apiKey;
  }

  public String getRegistrationCaCertificate() {
    return registrationCaCertificate;
  }

  public void setRegistrationCaCertificate(final String registrationCaCertificate) {
    this.registrationCaCertificate = registrationCaCertificate;
  }
}
