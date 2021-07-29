/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class DynamicPreRegistrationExperimentEnrollmentConfiguration {

  @JsonProperty
  @Valid
  private Set<String> enrolledE164s = Collections.emptySet();

  @JsonProperty
  @Valid
  private Set<String> excludedE164s = Collections.emptySet();

  @JsonProperty
  @Valid
  private Set<String> includedCountryCodes = Collections.emptySet();

  @JsonProperty
  @Valid
  private Set<String> excludedCountryCodes = Collections.emptySet();

  @JsonProperty
  @Valid
  @Min(0)
  @Max(100)
  private int enrollmentPercentage = 0;

  public Set<String> getEnrolledE164s() {
    return enrolledE164s;
  }

  public Set<String> getExcludedE164s() {
    return excludedE164s;
  }

  public Set<String> getIncludedCountryCodes() {
    return includedCountryCodes;
  }

  public Set<String> getExcludedCountryCodes() {
    return excludedCountryCodes;
  }

  public int getEnrollmentPercentage() {
    return enrollmentPercentage;
  }
}
