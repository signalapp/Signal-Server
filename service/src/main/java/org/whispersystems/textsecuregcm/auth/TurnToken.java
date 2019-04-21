package org.whispersystems.textsecuregcm.auth;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class TurnToken {

  @JsonProperty
  private String username;

  @JsonProperty
  private String password;

  @JsonProperty
  private List<String> urls;

  public TurnToken(String username, String password, List<String> urls) {
    this.username = username;
    this.password = password;
    this.urls     = urls;
  }
}
