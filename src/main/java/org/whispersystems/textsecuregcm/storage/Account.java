/**
 * Copyright (C) 2013 Open WhisperSystems
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
package org.whispersystems.textsecuregcm.storage;


import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Account implements Serializable {
  private String  number;
  private boolean supportsSms;
  private Map<Long, Device> devices = new HashMap<>();

  private Account(String number, boolean supportsSms) {
    this.number      = number;
    this.supportsSms = supportsSms;
  }

  public Account(String number, boolean supportsSms, Device onlyDevice) {
    this(number, supportsSms);
    this.devices.put(onlyDevice.getDeviceId(), onlyDevice);
  }

  public Account(String number, boolean supportsSms, List<Device> devices) {
    this(number, supportsSms);
    for (Device device : devices)
      this.devices.put(device.getDeviceId(), device);
  }

  public void setNumber(String number) {
    this.number = number;
  }

  public String getNumber() {
    return number;
  }

  public boolean getSupportsSms() {
    return supportsSms;
  }

  public void setSupportsSms(boolean supportsSms) {
    this.supportsSms = supportsSms;
  }

  public boolean isActive() {
    Device masterDevice = devices.get((long) 1);
    return masterDevice != null && masterDevice.isActive();
  }

  public Collection<Device> getDevices() {
    return devices.values();
  }

  public Device getDevice(long destinationDeviceId) {
    return devices.get(destinationDeviceId);
  }

  public boolean hasAllDeviceIds(Set<Long> deviceIds) {
    if (devices.size() != deviceIds.size())
      return false;
    for (long deviceId : devices.keySet()) {
      if (!deviceIds.contains(deviceId))
        return false;
    }
    return true;
  }
}
