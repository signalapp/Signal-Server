package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Set;
import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotNull;

public class DynamicCaptchaConfiguration {

  @JsonProperty
  @DecimalMin("0")
  @DecimalMax("1")
  @NotNull
  private BigDecimal scoreFloor;

  @JsonProperty
  @NotNull
  private Set<String> signupCountryCodes = Collections.emptySet();

  @JsonProperty
  @NotNull
  private Set<String> signupRegions = Collections.emptySet();

  public BigDecimal getScoreFloor() {
    return scoreFloor;
  }

  public Set<String> getSignupCountryCodes() {
    return signupCountryCodes;
  }

  @VisibleForTesting
  public void setSignupCountryCodes(Set<String> numbers) {
    this.signupCountryCodes = numbers;
  }

  @VisibleForTesting
  public void setSignupRegions(final Set<String> signupRegions) {
    this.signupRegions = signupRegions;
  }

  public Set<String> getSignupRegions() {
    return signupRegions;
  }
}
