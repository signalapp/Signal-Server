package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

public class Profile {

  @JsonProperty
  private String identityKey;

  public Profile() {}

  public Profile(String identityKey) {
    this.identityKey = identityKey;
  }

  @VisibleForTesting
  public String getIdentityKey() {
    return identityKey;
  }
}
