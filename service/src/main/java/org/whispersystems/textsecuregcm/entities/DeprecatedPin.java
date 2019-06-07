package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.constraints.NotEmpty;

public class DeprecatedPin {

  @JsonProperty
  @NotEmpty
  @Length(min=4,max=20)
  private String pin;

  public DeprecatedPin() {}

  @VisibleForTesting
  public DeprecatedPin(String pin) {
    this.pin = pin;
  }

  public String getPin() {
    return pin;
  }

}
