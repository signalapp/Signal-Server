package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.validation.constraints.NotNull;

public class RateLimitChallenge {

  @JsonProperty
  @NotNull
  @Schema(description="An opaque token to be included along with the challenge result in the verification request")
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
