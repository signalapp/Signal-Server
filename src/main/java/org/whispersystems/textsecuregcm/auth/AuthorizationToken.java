package org.whispersystems.textsecuregcm.auth;


import com.fasterxml.jackson.annotation.JsonProperty;

public class AuthorizationToken {

  @JsonProperty
  private String username;

  @JsonProperty
  private String password;

  public AuthorizationToken(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public AuthorizationToken() {}

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
