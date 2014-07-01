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


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class Account implements Serializable {

  public static final int MEMCACHE_VERION = 4;

  @JsonIgnore
  private long id;

  @JsonProperty
  private String number;

  @JsonProperty
  private boolean supportsSms;

  @JsonProperty
  private List<Device> devices = new LinkedList<>();

  @JsonProperty
  private String identityKey;

  @JsonIgnore
  private Optional<Device> authenticatedDevice;

  public Account() {}

  @VisibleForTesting
  public Account(String number, boolean supportsSms, List<Device> devices) {
    this.number      = number;
    this.supportsSms = supportsSms;
    this.devices     = devices;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public Optional<Device> getAuthenticatedDevice() {
    return authenticatedDevice;
  }

  public void setAuthenticatedDevice(Device device) {
    this.authenticatedDevice = Optional.of(device);
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

  public void addDevice(Device device) {
    this.devices.add(device);
  }

  public void setDevices(List<Device> devices) {
    this.devices = devices;
  }

  public List<Device> getDevices() {
    return devices;
  }

  public Optional<Device> getMasterDevice() {
    return getDevice(Device.MASTER_ID);
  }

  public Optional<Device> getDevice(long deviceId) {
    for (Device device : devices) {
      if (device.getId() == deviceId) {
        return Optional.of(device);
      }
    }

    return Optional.absent();
  }

  public boolean isActive() {
    return
        getMasterDevice().isPresent() &&
        getMasterDevice().get().isActive();
  }

  public long getNextDeviceId() {
    long highestDevice = Device.MASTER_ID;

    for (Device device : devices) {
      if (device.getId() > highestDevice) {
        highestDevice = device.getId();
      }
    }

    return highestDevice + 1;
  }

  public boolean isRateLimited() {
    return true;
  }

  public Optional<String> getRelay() {
    return Optional.absent();
  }

  public void setIdentityKey(String identityKey) {
    this.identityKey = identityKey;
  }

  public String getIdentityKey() {
    return identityKey;
  }
}
