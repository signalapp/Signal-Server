package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;

public class DynamicAbusiveHostRulesConfiguration {

  @JsonProperty
  private Duration expirationTime = Duration.ofDays(1);

  public Duration getExpirationTime() {
    return expirationTime;
  }
}
