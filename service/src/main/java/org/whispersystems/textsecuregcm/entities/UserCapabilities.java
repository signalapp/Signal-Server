package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UserCapabilities {
  @JsonProperty
  private boolean uuid;

  @JsonProperty
  private boolean gv2;

  public UserCapabilities() {}

  public UserCapabilities(boolean uuid, boolean gv2) {
    this.uuid = uuid;
    this.gv2  = gv2;
  }

  public boolean isUuid() {
    return uuid;
  }

  public boolean isGv2() {
    return gv2;
  }
}
