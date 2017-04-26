package org.whispersystems.textsecuregcm.push;

public class ApnMessage {

  public static long MAX_EXPIRATION = Integer.MAX_VALUE * 1000L;

  private final String apnId;
  private final String number;
  private final int deviceId;
  private final String message;
  private final boolean voip;
  private final long expirationTime;

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

  public String getApnId() {
    return apnId;
  }

  public boolean isVoip() {
    return voip;
  }

  public String getMessage() {
    return message;
  }

  public long getExpirationTime() {
    return expirationTime;
  }

  public String getNumber() {
    return number;
  }

  public int getDeviceId() {
    return deviceId;
  }
}
