package org.whispersystems.textsecuregcm.entities;

/**
 * Copyright (C) 2014 Open Whisper Systems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

public class PreKey {

  @JsonProperty
  @NotNull
  private long    keyId;

  @JsonProperty
  @NotEmpty
  private String  publicKey;

  public PreKey() {}

  public PreKey(long keyId, String publicKey)
  {
    this.keyId     = keyId;
    this.publicKey = publicKey;
  }

  public String getPublicKey() {
    return publicKey;
  }

  public void setPublicKey(String publicKey) {
    this.publicKey = publicKey;
  }

  public long getKeyId() {
    return keyId;
  }

  public void setKeyId(long keyId) {
    this.keyId = keyId;
  }

  @Override
  public boolean equals(Object object) {
    if (object == null || !(object instanceof PreKey)) return false;
    PreKey that = (PreKey)object;

    if (publicKey == null) {
      return this.keyId == that.keyId && that.publicKey == null;
    } else {
      return this.keyId == that.keyId && this.publicKey.equals(that.publicKey);
    }
  }

  @Override
  public int hashCode() {
    if (publicKey == null) {
      return (int)this.keyId;
    } else {
      return ((int)this.keyId) ^ publicKey.hashCode();
    }
  }

}
