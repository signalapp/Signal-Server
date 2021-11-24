package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.validation.Valid;

public class DynamicConfiguration {

  @JsonProperty
  @Valid
  private Map<String, DynamicExperimentEnrollmentConfiguration> experiments = Collections.emptyMap();

  @JsonProperty
  @Valid
  private Map<String, DynamicPreRegistrationExperimentEnrollmentConfiguration> preRegistrationExperiments = Collections.emptyMap();

  @JsonProperty
  @Valid
  private DynamicRateLimitsConfiguration limits = new DynamicRateLimitsConfiguration();

  @JsonProperty
  @Valid
  private DynamicRemoteDeprecationConfiguration remoteDeprecation = new DynamicRemoteDeprecationConfiguration();

  @JsonProperty
  @Valid
  private DynamicMessageRateConfiguration messageRate = new DynamicMessageRateConfiguration();

  @JsonProperty
  @Valid
  private DynamicPaymentsConfiguration payments = new DynamicPaymentsConfiguration();

  @JsonProperty
  private Set<String> featureFlags = Collections.emptySet();

  @JsonProperty
  @Valid
  private DynamicTwilioConfiguration twilio = new DynamicTwilioConfiguration();

  @JsonProperty
  private DynamicSignupCaptchaConfiguration signupCaptcha = new DynamicSignupCaptchaConfiguration();

  @JsonProperty
  @Valid
  private DynamicRateLimitChallengeConfiguration rateLimitChallenge = new DynamicRateLimitChallengeConfiguration();

  @JsonProperty
  private DynamicDirectoryReconcilerConfiguration directoryReconciler = new DynamicDirectoryReconcilerConfiguration();

  @JsonProperty
  @Valid
  private DynamicPushLatencyConfiguration pushLatency = new DynamicPushLatencyConfiguration(Collections.emptyMap());

  @JsonProperty
  @Valid
  private DynamicProfileMigrationConfiguration profileMigration = new DynamicProfileMigrationConfiguration();

  public Optional<DynamicExperimentEnrollmentConfiguration> getExperimentEnrollmentConfiguration(
      final String experimentName) {
    return Optional.ofNullable(experiments.get(experimentName));
  }

  public Optional<DynamicPreRegistrationExperimentEnrollmentConfiguration> getPreRegistrationEnrollmentConfiguration(
      final String experimentName) {
    return Optional.ofNullable(preRegistrationExperiments.get(experimentName));
  }

  public DynamicRateLimitsConfiguration getLimits() {
    return limits;
  }

  public DynamicRemoteDeprecationConfiguration getRemoteDeprecationConfiguration() {
    return remoteDeprecation;
  }

  public DynamicMessageRateConfiguration getMessageRateConfiguration() {
    return messageRate;
  }

  public DynamicPaymentsConfiguration getPaymentsConfiguration() {
    return payments;
  }

  public Set<String> getActiveFeatureFlags() {
    return featureFlags;
  }

  public DynamicTwilioConfiguration getTwilioConfiguration() {
    return twilio;
  }

  @VisibleForTesting
  public void setTwilioConfiguration(DynamicTwilioConfiguration twilioConfiguration) {
    this.twilio = twilioConfiguration;
  }

  public DynamicSignupCaptchaConfiguration getSignupCaptchaConfiguration() {
    return signupCaptcha;
  }

  public DynamicRateLimitChallengeConfiguration getRateLimitChallengeConfiguration() {
    return rateLimitChallenge;
  }

  public DynamicDirectoryReconcilerConfiguration getDirectoryReconcilerConfiguration() {
    return directoryReconciler;
  }

  public DynamicPushLatencyConfiguration getPushLatencyConfiguration() {
    return pushLatency;
  }

  public DynamicProfileMigrationConfiguration getProfileMigrationConfiguration() {
    return profileMigration;
  }
}
