package org.whispersystems.textsecuregcm.push;

public class ApnMessage {

  public static final String APN_PAYLOAD    = "{\"aps\":{\"sound\":\"default\",\"alert\":{\"loc-key\":\"APN_Message\"}}}";
  public static final long   MAX_EXPIRATION = Integer.MAX_VALUE * 1000L;

  private final String apnId;
  private final String number;
  private final long   deviceId;
  private final boolean isVoip;

  public ApnMessage(String apnId, String number, long deviceId, boolean isVoip) {
    this.apnId          = apnId;
    this.number         = number;
    this.deviceId       = deviceId;
    this.isVoip         = isVoip;
  }
  
  public boolean isVoip() {
    return isVoip;
  }

  public String getApnId() {
    return apnId;
  }

  public String getMessage() {
    return APN_PAYLOAD;
  }

  public long getExpirationTime() {
    return MAX_EXPIRATION;
  }

  public String getNumber() {
    return number;
  }

  public long getDeviceId() {
    return deviceId;
  }
}
