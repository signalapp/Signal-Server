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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

public class PreKeyResponseItemV2 {

  @JsonProperty
  private long deviceId;

  @JsonProperty
  private int registrationId;

  @JsonProperty
  private SignedPreKey signedPreKey;

  @JsonProperty
  private PreKeyV2 preKey;

  public PreKeyResponseItemV2() {}

  public PreKeyResponseItemV2(long deviceId, int registrationId, SignedPreKey signedPreKey, PreKeyV2 preKey) {
    this.deviceId       = deviceId;
    this.registrationId = registrationId;
    this.signedPreKey   = signedPreKey;
    this.preKey         = preKey;
  }

  @VisibleForTesting
  public SignedPreKey getSignedPreKey() {
    return signedPreKey;
  }

  @VisibleForTesting
  public PreKeyV2 getPreKey() {
    return preKey;
  }

  @VisibleForTesting
  public int getRegistrationId() {
    return registrationId;
  }

  @VisibleForTesting
  public long getDeviceId() {
    return deviceId;
  }
}
