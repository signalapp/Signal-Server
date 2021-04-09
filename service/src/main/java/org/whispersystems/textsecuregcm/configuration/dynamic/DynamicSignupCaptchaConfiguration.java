package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.Set;
import javax.validation.constraints.NotNull;

public class DynamicSignupCaptchaConfiguration {

  @JsonProperty
  @NotNull
  private Set<String> countryCodes = Collections.emptySet();

  public Set<String> getCountryCodes() {
    return countryCodes;
  }

  @VisibleForTesting
  public void setCountryCodes(Set<String> numbers) {
    this.countryCodes = numbers;
  }
}
