package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;

import org.signal.zkgroup.profiles.ProfileKeyCredentialResponse;

import java.util.UUID;

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

  @JsonProperty
  private UserCapabilities capabilities;

  @JsonProperty
  private String username;

  @JsonProperty
  private UUID uuid;

  @JsonProperty
  @JsonSerialize(using = ProfileKeyCredentialResponseAdapter.Serializing.class)
  @JsonDeserialize(using = ProfileKeyCredentialResponseAdapter.Deserializing.class)
  private ProfileKeyCredentialResponse credential;

  public Profile() {}

  public Profile(String name, String avatar, String identityKey,
                 String unidentifiedAccess, boolean unrestrictedUnidentifiedAccess,
                 UserCapabilities capabilities, String username, UUID uuid,
                 ProfileKeyCredentialResponse credential)
  {
    this.name                           = name;
    this.avatar                         = avatar;
    this.identityKey                    = identityKey;
    this.unidentifiedAccess             = unidentifiedAccess;
    this.unrestrictedUnidentifiedAccess = unrestrictedUnidentifiedAccess;
    this.capabilities                   = capabilities;
    this.username                       = username;
    this.uuid                           = uuid;
    this.credential                     = credential;
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

  @VisibleForTesting
  public UserCapabilities getCapabilities() {
    return capabilities;
  }

  @VisibleForTesting
  public String getUsername() {
    return username;
  }

  @VisibleForTesting
  public UUID getUuid() {
    return uuid;
  }
}
