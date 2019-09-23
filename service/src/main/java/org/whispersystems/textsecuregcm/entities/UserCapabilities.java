package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UserCapabilities {
  @JsonProperty
  private boolean uuid;

  public UserCapabilities() {}

  public UserCapabilities(boolean uuid) {
    this.uuid = uuid;
  }

  public boolean isUuid() {
    return uuid;
  }
}
