package org.whispersystems.textsecuregcm.auth;


import com.fasterxml.jackson.annotation.JsonProperty;

public class ExternalServiceCredentials {

  @JsonProperty
  private String username;

  @JsonProperty
  private String password;

  public ExternalServiceCredentials(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public ExternalServiceCredentials() {}

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
