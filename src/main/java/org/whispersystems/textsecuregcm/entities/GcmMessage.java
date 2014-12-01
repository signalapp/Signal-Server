package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public class GcmMessage {

  @JsonProperty
  @NotEmpty
  private String gcmId;

  @JsonProperty
  @NotEmpty
  private String number;

  @JsonProperty
  @Min(1)
  private int deviceId;

  @JsonProperty
  @NotEmpty
  private String message;

  @JsonProperty
  private boolean receipt;

  public GcmMessage() {}

  public GcmMessage(String gcmId, String number, int deviceId, String message, boolean receipt) {
    this.gcmId    = gcmId;
    this.number   = number;
    this.deviceId = deviceId;
    this.message  = message;
    this.receipt  = receipt;
  }

}
