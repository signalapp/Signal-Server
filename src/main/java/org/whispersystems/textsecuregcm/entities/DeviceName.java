package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.constraints.NotEmpty;

public class DeviceName {

  @JsonProperty
  @NotEmpty
  @Length(max = 300, message = "This field must be less than 300 characters")
  private String deviceName;

  public DeviceName() {}

  public String getDeviceName() {
    return deviceName;
  }
}
