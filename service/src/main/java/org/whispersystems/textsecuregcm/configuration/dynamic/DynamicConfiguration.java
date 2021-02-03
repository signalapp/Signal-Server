package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class DynamicConfiguration {

  @JsonProperty
  @Valid
  private Map<String, DynamicExperimentEnrollmentConfiguration> experiments = Collections.emptyMap();

  @JsonProperty
  @Valid
  private DynamicRateLimitsConfiguration limits = new DynamicRateLimitsConfiguration();

  public Optional<DynamicExperimentEnrollmentConfiguration> getExperimentEnrollmentConfiguration(final String experimentName) {
    return Optional.ofNullable(experiments.get(experimentName));
  }

  public DynamicRateLimitsConfiguration getLimits() {
    return limits;
  }
}
