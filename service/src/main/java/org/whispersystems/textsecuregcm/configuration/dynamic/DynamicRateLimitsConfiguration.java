package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.CardinalityRateLimitConfiguration;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.RateLimitConfiguration;
import java.time.Duration;

public class DynamicRateLimitsConfiguration {

  @JsonProperty
  private CardinalityRateLimitConfiguration unsealedSenderNumber = new CardinalityRateLimitConfiguration(100, Duration.ofDays(1));

  @JsonProperty
  private int unsealedSenderDefaultCardinalityLimit = 100;

  @JsonProperty
  private int unsealedSenderPermitIncrement = 50;

  @JsonProperty
  private RateLimitConfiguration unsealedSenderIp = new RateLimitConfiguration(120, 2.0 / 60);

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

  @JsonProperty
  private RateLimitConfiguration dailyPreKeys = new RateLimitConfiguration(50, 50.0 / (24.0 * 60));

  public RateLimitConfiguration getUnsealedSenderIp() {
    return unsealedSenderIp;
  }

  public CardinalityRateLimitConfiguration getUnsealedSenderNumber() {
    return unsealedSenderNumber;
  }

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

  public int getUnsealedSenderDefaultCardinalityLimit() {
    return unsealedSenderDefaultCardinalityLimit;
  }

  public int getUnsealedSenderPermitIncrement() {
    return unsealedSenderPermitIncrement;
  }

  public RateLimitConfiguration getDailyPreKeys() {
    return dailyPreKeys;
  }
}
