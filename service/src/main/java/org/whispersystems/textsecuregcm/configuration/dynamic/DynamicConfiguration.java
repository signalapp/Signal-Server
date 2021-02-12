package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DynamicConfiguration {

  @JsonProperty
  @Valid
  private Map<String, DynamicExperimentEnrollmentConfiguration> experiments = Collections.emptyMap();

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
  private Set<String> featureFlags = Collections.emptySet();

  public Optional<DynamicExperimentEnrollmentConfiguration> getExperimentEnrollmentConfiguration(final String experimentName) {
    return Optional.ofNullable(experiments.get(experimentName));
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

  public Set<String> getActiveFeatureFlags() {
    return featureFlags;
  }
}
