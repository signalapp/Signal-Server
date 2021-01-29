package org.whispersystems.textsecuregcm.storage;

public class KeyRecord {

  private final long    id;
  private final String  number;
  private final long    deviceId;
  private final long    keyId;
  private final String  publicKey;

  public KeyRecord(long id, String number, long deviceId, long keyId, String publicKey) {
    this.id         = id;
    this.number     = number;
    this.deviceId   = deviceId;
    this.keyId      = keyId;
    this.publicKey  = publicKey;
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

}
