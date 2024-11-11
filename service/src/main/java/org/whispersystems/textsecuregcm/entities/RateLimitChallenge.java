package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public class RateLimitChallenge {

  @JsonProperty
  @NotNull
  private final String token;

  @JsonProperty
  @NotNull
  private final List<String> options;

  @JsonCreator
  public RateLimitChallenge(@JsonProperty("token") final String token, @JsonProperty("options") final List<String> options) {

    this.token = token;
    this.options = options;
  }

  public String getToken() {
    return token;
  }

  public List<String> getOptions() {
    return options;
  }
}
