package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WebsocketConfiguration {

  @JsonProperty
  private boolean enabled = false;

  public boolean isEnabled() {
    return enabled;
  }

}
