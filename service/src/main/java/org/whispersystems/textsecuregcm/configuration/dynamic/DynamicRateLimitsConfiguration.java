package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.CardinalityRateLimitConfiguration;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.RateLimitConfiguration;
import java.time.Duration;

public class DynamicRateLimitsConfiguration {

  @JsonProperty
  private CardinalityRateLimitConfiguration unsealedSenderNumber = new CardinalityRateLimitConfiguration(100, Duration.ofDays(1), Duration.ofDays(1));

  @JsonProperty
  private RateLimitConfiguration unsealedSenderIp = new RateLimitConfiguration(120, 2.0 / 60);

  public RateLimitConfiguration getUnsealedSenderIp() {
    return unsealedSenderIp;
  }

  public CardinalityRateLimitConfiguration getUnsealedSenderNumber() {
    return unsealedSenderNumber;
  }
}
