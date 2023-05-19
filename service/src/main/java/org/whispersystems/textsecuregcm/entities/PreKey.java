/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.Objects;

public class PreKey {

  @JsonProperty
  @NotNull
  private long keyId;

  @JsonProperty
  @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
  @NotEmpty
  private byte[] publicKey;

  public PreKey() {}

  public PreKey(long keyId, byte[] publicKey)
  {
    this.keyId = keyId;
    this.publicKey = publicKey;
  }

  public byte[] getPublicKey() {
    return publicKey;
  }

  public void setPublicKey(byte[] publicKey) {
    this.publicKey = publicKey;
  }

  public long getKeyId() {
    return keyId;
  }

  public void setKeyId(long keyId) {
    this.keyId = keyId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PreKey preKey = (PreKey) o;
    return keyId == preKey.keyId && Arrays.equals(publicKey, preKey.publicKey);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(keyId);
    result = 31 * result + Arrays.hashCode(publicKey);
    return result;
  }
}
