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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;

public class PreKeyResponse {

  @JsonProperty
  private String identityKey;

  @JsonProperty
  private List<PreKeyResponseItem> devices;

  public PreKeyResponse() {}

  public PreKeyResponse(String identityKey, List<PreKeyResponseItem> devices) {
    this.identityKey = identityKey;
    this.devices     = devices;
  }

  @VisibleForTesting
  public String getIdentityKey() {
    return identityKey;
  }

  @VisibleForTesting
  @JsonIgnore
  public PreKeyResponseItem getDevice(int deviceId) {
    for (PreKeyResponseItem device : devices) {
      if (device.getDeviceId() == deviceId) return device;
    }

    return null;
  }

  @VisibleForTesting
  @JsonIgnore
  public int getDevicesCount() {
    return devices.size();
  }

}
