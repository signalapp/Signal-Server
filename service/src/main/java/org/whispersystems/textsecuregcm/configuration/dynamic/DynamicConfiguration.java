/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.vdurmont.semver4j.Semver;
import jakarta.validation.Valid;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.whispersystems.textsecuregcm.limits.RateLimiterConfig;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

public class DynamicConfiguration {

  @JsonProperty
  @Valid
  private Map<String, DynamicExperimentEnrollmentConfiguration> experiments = Collections.emptyMap();

  @JsonProperty
  @Valid
  private Map<String, DynamicE164ExperimentEnrollmentConfiguration> e164Experiments = Collections.emptyMap();

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
  DynamicMessagePersisterConfiguration messagePersister = new DynamicMessagePersisterConfiguration();

  @JsonProperty
  @Valid
  DynamicRegistrationConfiguration registrationConfiguration = new DynamicRegistrationConfiguration(false);

  @JsonProperty
  @Valid
  DynamicVirtualThreadConfiguration virtualThreads = new DynamicVirtualThreadConfiguration(Collections.emptySet());

  @JsonProperty
  @Valid
  DynamicMetricsConfiguration metricsConfiguration = new DynamicMetricsConfiguration(false, false);

  @JsonProperty
  @Valid
  List<String> svrStatusCodesToIgnoreForAccountDeletion = Collections.emptyList();

  @JsonProperty
  @Valid
  DynamicRestDeprecationConfiguration restDeprecation = new DynamicRestDeprecationConfiguration(Map.of());

  public Optional<DynamicExperimentEnrollmentConfiguration> getExperimentEnrollmentConfiguration(
      final String experimentName) {
    return Optional.ofNullable(experiments.get(experimentName));
  }

  public Optional<DynamicE164ExperimentEnrollmentConfiguration> getE164ExperimentEnrollmentConfiguration(
      final String experimentName) {
    return Optional.ofNullable(e164Experiments.get(experimentName));
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

  public DynamicMessagePersisterConfiguration getMessagePersisterConfiguration() {
    return messagePersister;
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

  public List<String> getSvrStatusCodesToIgnoreForAccountDeletion() {
    return svrStatusCodesToIgnoreForAccountDeletion;
  }

  public DynamicRestDeprecationConfiguration restDeprecation() {
    return restDeprecation;
  }

}
