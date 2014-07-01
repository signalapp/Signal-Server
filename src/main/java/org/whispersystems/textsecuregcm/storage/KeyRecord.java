package org.whispersystems.textsecuregcm.storage;

public class KeyRecord {

  private long    id;
  private String  number;
  private long    deviceId;
  private long    keyId;
  private String  publicKey;
  private boolean lastResort;

  public KeyRecord(long id, String number, long deviceId, long keyId,
                   String publicKey, boolean lastResort)
  {
    this.id         = id;
    this.number     = number;
    this.deviceId   = deviceId;
    this.keyId      = keyId;
    this.publicKey  = publicKey;
    this.lastResort = lastResort;
  }

  public long getId() {
    return id;
  }

  public String getNumber() {
    return number;
  }

  public long getDeviceId() {
    return deviceId;
  }

  public long getKeyId() {
    return keyId;
  }

  public String getPublicKey() {
    return publicKey;
  }

  public boolean isLastResort() {
    return lastResort;
  }
}
