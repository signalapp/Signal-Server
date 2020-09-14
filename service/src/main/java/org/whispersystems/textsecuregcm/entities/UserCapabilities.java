package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UserCapabilities {
  @JsonProperty
  private boolean gv2;

  public UserCapabilities() {}

  public UserCapabilities(boolean gv2) {
    this.gv2  = gv2;
  }

  public boolean isGv2() {
    return gv2;
  }
}
