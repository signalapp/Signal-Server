package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class ApnMessage {

  public static long MAX_EXPIRATION = Integer.MAX_VALUE * 1000L;

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

  @JsonProperty
  @NotNull
  private boolean voip;

  @JsonProperty
  private long expirationTime;

  public ApnMessage() {}

  public ApnMessage(String apnId, String number, int deviceId, String message, boolean voip, long expirationTime) {
    this.apnId          = apnId;
    this.number         = number;
    this.deviceId       = deviceId;
    this.message        = message;
    this.voip           = voip;
    this.expirationTime = expirationTime;
  }

  public ApnMessage(ApnMessage copy, String apnId, boolean voip, long expirationTime) {
    this.apnId          = apnId;
    this.number         = copy.number;
    this.deviceId       = copy.deviceId;
    this.message        = copy.message;
    this.voip           = voip;
    this.expirationTime = expirationTime;
  }

  @VisibleForTesting
  public String getApnId() {
    return apnId;
  }

  @VisibleForTesting
  public boolean isVoip() {
    return voip;
  }

  @VisibleForTesting
  public String getMessage() {
    return message;
  }

  @VisibleForTesting
  public long getExpirationTime() {
    return expirationTime;
  }
}
