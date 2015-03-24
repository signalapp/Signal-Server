package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public class UnregisteredEvent {

  @JsonProperty
  @NotEmpty
  private String registrationId;

  @JsonProperty
  private String canonicalId;

  @JsonProperty
  @NotEmpty
  private String number;

  @JsonProperty
  @Min(1)
  private int deviceId;

  @JsonProperty
  private long timestamp;

  public String getRegistrationId() {
    return registrationId;
  }

  public String getCanonicalId() {
    return canonicalId;
  }

  public String getNumber() {
    return number;
  }

  public int getDeviceId() {
    return deviceId;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
