package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.RateLimitConfiguration;

public class DynamicRateLimitsConfiguration {

  @JsonProperty
  private RateLimitConfiguration rateLimitReset = new RateLimitConfiguration(2, 2.0 / (60 * 24));

  @JsonProperty
  private RateLimitConfiguration recaptchaChallengeAttempt = new RateLimitConfiguration(10, 10.0 / (60 * 24));

  @JsonProperty
  private RateLimitConfiguration recaptchaChallengeSuccess = new RateLimitConfiguration(2, 2.0 / (60 * 24));

  @JsonProperty
  private RateLimitConfiguration pushChallengeAttempt = new RateLimitConfiguration(10, 10.0 / (60 * 24));

  @JsonProperty
  private RateLimitConfiguration pushChallengeSuccess = new RateLimitConfiguration(2, 2.0 / (60 * 24));

  public RateLimitConfiguration getRateLimitReset() {
    return rateLimitReset;
  }

  public RateLimitConfiguration getRecaptchaChallengeAttempt() {
    return recaptchaChallengeAttempt;
  }

  public RateLimitConfiguration getRecaptchaChallengeSuccess() {
    return recaptchaChallengeSuccess;
  }

  public RateLimitConfiguration getPushChallengeAttempt() {
    return pushChallengeAttempt;
  }

  public RateLimitConfiguration getPushChallengeSuccess() {
    return pushChallengeSuccess;
  }
}
