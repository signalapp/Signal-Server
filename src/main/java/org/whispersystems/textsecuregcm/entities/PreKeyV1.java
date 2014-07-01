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
package org.whispersystems.textsecuregcm.entities;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

import javax.validation.constraints.NotNull;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class PreKeyV1 implements PreKeyBase {

  @JsonProperty
  private long deviceId;

  @JsonProperty
  @NotNull
  private long    keyId;

  @JsonProperty
  @NotNull
  private String  publicKey;

  @JsonProperty
  @NotNull
  private String  identityKey;

  @JsonProperty
  private int registrationId;

  public PreKeyV1() {}

  public PreKeyV1(long deviceId, long keyId, String publicKey, String identityKey, int registrationId)
  {
    this.deviceId       = deviceId;
    this.keyId          = keyId;
    this.publicKey      = publicKey;
    this.identityKey    = identityKey;
    this.registrationId = registrationId;
  }

  @VisibleForTesting
  public PreKeyV1(long deviceId, long keyId, String publicKey, String identityKey)
  {
    this.deviceId    = deviceId;
    this.keyId       = keyId;
    this.publicKey   = publicKey;
    this.identityKey = identityKey;
  }

  @Override
  public String getPublicKey() {
    return publicKey;
  }

  @Override
  public long getKeyId() {
    return keyId;
  }

  public String getIdentityKey() {
    return identityKey;
  }

  public void setDeviceId(long deviceId) {
    this.deviceId = deviceId;
  }

  public long getDeviceId() {
    return deviceId;
  }

  public int getRegistrationId() {
    return registrationId;
  }

  public void setRegistrationId(int registrationId) {
    this.registrationId = registrationId;
  }
}
