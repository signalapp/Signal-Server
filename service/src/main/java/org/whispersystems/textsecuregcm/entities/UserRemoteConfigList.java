package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class UserRemoteConfigList {

  @JsonProperty
  private List<UserRemoteConfig> config;

  public UserRemoteConfigList() {}

  public UserRemoteConfigList(List<UserRemoteConfig> config) {
    this.config = config;
  }

  public List<UserRemoteConfig> getConfig() {
    return config;
  }
}
