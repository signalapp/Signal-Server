/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.validation.Valid;
import org.whispersystems.textsecuregcm.limits.RateLimiterConfig;

public class DynamicConfiguration {

  @JsonProperty
  @Valid
  private Map<String, DynamicExperimentEnrollmentConfiguration> experiments = Collections.emptyMap();

  @JsonProperty
  @Valid
  private Map<String, DynamicPreRegistrationExperimentEnrollmentConfiguration> preRegistrationExperiments = Collections.emptyMap();

  @JsonProperty
  @Valid
  private Map<String, RateLimiterConfig> limits = new HashMap<>();

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
  private DynamicTurnConfiguration turn = new DynamicTurnConfiguration();

  @JsonProperty
  @Valid
  DynamicMessagePersisterConfiguration messagePersister = new DynamicMessagePersisterConfiguration();

  @JsonProperty
  @Valid
  DynamicRateLimitPolicy rateLimitPolicy = new DynamicRateLimitPolicy(false);

  @JsonProperty
  @Valid
  DynamicInboundMessageByteLimitConfiguration inboundMessageByteLimit = new DynamicInboundMessageByteLimitConfiguration(true);

  @JsonProperty
  @Valid
  DynamicRegistrationConfiguration registrationConfiguration = new DynamicRegistrationConfiguration(false);

  @JsonProperty
  @Valid
  DynamicVirtualThreadConfiguration virtualThreads = new DynamicVirtualThreadConfiguration(Collections.emptySet());

  @JsonProperty
  @Valid
  DynamicMetricsConfiguration metricsConfiguration = new DynamicMetricsConfiguration(false);

  @JsonProperty
  @Valid
  DynamicMessagesConfiguration messagesConfiguration = new DynamicMessagesConfiguration();

  @JsonProperty
  @Valid
  List<String> svrStatusCodesToIgnoreForAccountDeletion = Collections.emptyList();

  public Optional<DynamicExperimentEnrollmentConfiguration> getExperimentEnrollmentConfiguration(
      final String experimentName) {
    return Optional.ofNullable(experiments.get(experimentName));
  }

  public Optional<DynamicPreRegistrationExperimentEnrollmentConfiguration> getPreRegistrationEnrollmentConfiguration(
      final String experimentName) {
    return Optional.ofNullable(preRegistrationExperiments.get(experimentName));
  }

  public Map<String, RateLimiterConfig> getLimits() {
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

  public DynamicTurnConfiguration getTurnConfiguration() {
    return turn;
  }

  public DynamicMessagePersisterConfiguration getMessagePersisterConfiguration() {
    return messagePersister;
  }

  public DynamicRateLimitPolicy getRateLimitPolicy() {
    return rateLimitPolicy;
  }

  public DynamicInboundMessageByteLimitConfiguration getInboundMessageByteLimitConfiguration() {
    return inboundMessageByteLimit;
  }

  public DynamicRegistrationConfiguration getRegistrationConfiguration() {
    return registrationConfiguration;
  }

  public DynamicVirtualThreadConfiguration getVirtualThreads() {
    return virtualThreads;
  }

  public DynamicMetricsConfiguration getMetricsConfiguration() {
    return metricsConfiguration;
  }

  public DynamicMessagesConfiguration getMessagesConfiguration() {
    return messagesConfiguration;
  }

  public List<String> getSvrStatusCodesToIgnoreForAccountDeletion() {
    return svrStatusCodesToIgnoreForAccountDeletion;
  }

}
