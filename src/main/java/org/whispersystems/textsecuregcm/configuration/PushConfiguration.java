package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public class PushConfiguration {
  @JsonProperty
  @NotEmpty
  private String host;

  @JsonProperty
  @Min(1)
  private int port;

  @JsonProperty
  @NotEmpty
  private String username;

  @JsonProperty
  @NotEmpty
  private String password;

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
