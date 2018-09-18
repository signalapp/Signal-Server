package org.whispersystems.textsecuregcm.auth;


import com.fasterxml.jackson.annotation.JsonProperty;

public class DirectoryCredentials {

  @JsonProperty
  private String username;

  @JsonProperty
  private String password;

  public DirectoryCredentials(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public DirectoryCredentials() {}

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
