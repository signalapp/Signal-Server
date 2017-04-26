package org.whispersystems.textsecuregcm.push;

public class GcmMessage {

  private final String  gcmId;
  private final String  number;
  private final int     deviceId;
  private final boolean receipt;

  public GcmMessage(String gcmId, String number, int deviceId, boolean receipt) {
    this.gcmId        = gcmId;
    this.number       = number;
    this.deviceId     = deviceId;
    this.receipt      = receipt;
  }

  public String getGcmId() {
    return gcmId;
  }

  public String getNumber() {
    return number;
  }

  public boolean isReceipt() {
    return receipt;
  }

  public int getDeviceId() {
    return deviceId;
  }
}
