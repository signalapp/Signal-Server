package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
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
  private DynamicPaymentsConfiguration payments = new DynamicPaymentsConfiguration();

  @JsonProperty
  @Valid
  private DynamicCaptchaConfiguration captcha = new DynamicCaptchaConfiguration();

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
  private DynamicTurnConfiguration turn = new DynamicTurnConfiguration();

  @JsonProperty
  @Valid
  DynamicMessagePersisterConfiguration messagePersister = new DynamicMessagePersisterConfiguration();

  @JsonProperty
  @Valid
  DynamicPushNotificationConfiguration pushNotifications = new DynamicPushNotificationConfiguration();

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

  public DynamicPaymentsConfiguration getPaymentsConfiguration() {
    return payments;
  }

  public DynamicCaptchaConfiguration getCaptchaConfiguration() {
    return captcha;
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

  public DynamicTurnConfiguration getTurnConfiguration() {
    return turn;
  }

  public DynamicMessagePersisterConfiguration getMessagePersisterConfiguration() {
    return messagePersister;
  }

  public DynamicPushNotificationConfiguration getPushNotificationConfiguration() {
    return pushNotifications;
  }
}
