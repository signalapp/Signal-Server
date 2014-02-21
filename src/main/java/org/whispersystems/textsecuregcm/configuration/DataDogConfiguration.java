package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataDogConfiguration {

  @JsonProperty
  private String apiKey;

  @JsonProperty
  private boolean enabled = false;

  public String getApiKey() {
    return apiKey;
  }

  public boolean isEnabled() {
    return enabled && apiKey != null;
  }
}
