/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.Objects;

public class KeyRecord {

  private long    id;
  private String  number;
  private long    deviceId;
  private long    keyId;
  private String  publicKey;

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

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final KeyRecord keyRecord = (KeyRecord)o;
    return id == keyRecord.id &&
            deviceId == keyRecord.deviceId &&
            keyId == keyRecord.keyId &&
            Objects.equals(number, keyRecord.number) &&
            Objects.equals(publicKey, keyRecord.publicKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, number, deviceId, keyId, publicKey);
  }
}
