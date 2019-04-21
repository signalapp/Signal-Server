package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

import org.hibernate.validator.constraints.Length;

import javax.validation.constraints.Max;

public class Profile {

  @JsonProperty
  private String identityKey;

  @JsonProperty
  private String name;

  @JsonProperty
  private String avatar;

  @JsonProperty
  private String unidentifiedAccess;

  @JsonProperty
  private boolean unrestrictedUnidentifiedAccess;

  public Profile() {}

  public Profile(String name, String avatar, String identityKey, String unidentifiedAccess, boolean unrestrictedUnidentifiedAccess) {
    this.name                           = name;
    this.avatar                         = avatar;
    this.identityKey                    = identityKey;
    this.unidentifiedAccess             = unidentifiedAccess;
    this.unrestrictedUnidentifiedAccess = unrestrictedUnidentifiedAccess;
  }

  @VisibleForTesting
  public String getIdentityKey() {
    return identityKey;
  }

  @VisibleForTesting
  public String getName() {
    return name;
  }

  @VisibleForTesting
  public String getAvatar() {
    return avatar;
  }

  @VisibleForTesting
  public String getUnidentifiedAccess() {
    return unidentifiedAccess;
  }

  @VisibleForTesting
  public boolean isUnrestrictedUnidentifiedAccess() {
    return unrestrictedUnidentifiedAccess;
  }

}
