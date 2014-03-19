package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MetricsConfiguration {

  @JsonProperty
  private String token;

  @JsonProperty
  private String host;

  @JsonProperty
  private boolean enabled = false;

  public String getHost() {
    return host;
  }

  public String getToken() {
    return token;
  }

  public boolean isEnabled() {
    return enabled && token != null && host != null;
  }
}
