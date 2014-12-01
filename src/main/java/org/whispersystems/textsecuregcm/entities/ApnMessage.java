package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public class ApnMessage {
  @JsonProperty
  @NotEmpty
  private String apnId;

  @JsonProperty
  @NotEmpty
  private String number;

  @JsonProperty
  @Min(1)
  private int deviceId;

  @JsonProperty
  @NotEmpty
  private String message;

  public ApnMessage() {}

  public ApnMessage(String apnId, String number, int deviceId, String message) {
    this.apnId    = apnId;
    this.number   = number;
    this.deviceId = deviceId;
    this.message  = message;
  }
}
