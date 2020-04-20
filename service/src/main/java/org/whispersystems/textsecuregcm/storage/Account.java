/*
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
import org.whispersystems.textsecuregcm.auth.AmbiguousIdentifier;
import org.whispersystems.textsecuregcm.auth.StoredRegistrationLock;

import javax.security.auth.Subject;
import java.security.Principal;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Account implements Principal  {

  @JsonIgnore
  private UUID uuid;

  @JsonProperty
  private String number;

  @JsonProperty
  private Set<Device> devices = new HashSet<>();

  @JsonProperty
  private String identityKey;

  @JsonProperty
  private String name;

  @JsonProperty
  private String avatar;

  @JsonProperty
  private String avatarDigest;

  @JsonProperty
  private String pin;

  @JsonProperty
  private String registrationLock;

  @JsonProperty
  private String registrationLockSalt;

  @JsonProperty("uak")
  private byte[] unidentifiedAccessKey;

  @JsonProperty("uua")
  private boolean unrestrictedUnidentifiedAccess;

  @JsonIgnore
  private Device authenticatedDevice;

  public Account() {}

  @VisibleForTesting
  public Account(String number, UUID uuid, Set<Device> devices, byte[] unidentifiedAccessKey) {
    this.number                = number;
    this.uuid                  = uuid;
    this.devices               = devices;
    this.unidentifiedAccessKey = unidentifiedAccessKey;
  }

  public Optional<Device> getAuthenticatedDevice() {
    return Optional.ofNullable(authenticatedDevice);
  }

  public void setAuthenticatedDevice(Device device) {
    this.authenticatedDevice = device;
  }

  public UUID getUuid() {
    return uuid;
  }

  public void setUuid(UUID uuid) {
    this.uuid = uuid;
  }

  public void setNumber(String number) {
    this.number = number;
  }

  public String getNumber() {
    return number;
  }

  public void addDevice(Device device) {
    this.devices.remove(device);
    this.devices.add(device);
  }

  public void removeDevice(long deviceId) {
    this.devices.remove(new Device(deviceId, null, null, null, null, null, null, null, false, 0, null, 0, 0, "NA", 0, null));
  }

  public Set<Device> getDevices() {
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

    return Optional.empty();
  }

  public boolean isUuidAddressingSupported() {
    return devices.stream()
                  .filter(Device::isEnabled)
                  .allMatch(device -> device.getCapabilities() != null && device.getCapabilities().isUuid());
  }

  public boolean isGroupsV2Supported() {
    return devices.stream()
                  .filter(Device::isEnabled)
                  .anyMatch(device -> device.getCapabilities() != null && device.getCapabilities().isGv2());
  }

  public boolean isStorageSupported() {
    return devices.stream().anyMatch(device -> device.getCapabilities() != null && device.getCapabilities().isStorage());
  }

  public boolean isEnabled() {
    return
        getMasterDevice().isPresent()       &&
        getMasterDevice().get().isEnabled() &&
        getLastSeen() > (System.currentTimeMillis() - TimeUnit.DAYS.toMillis(365));
  }

  public long getNextDeviceId() {
    long highestDevice = Device.MASTER_ID;

    for (Device device : devices) {
      if (!device.isEnabled()) {
        return device.getId();
      } else if (device.getId() > highestDevice) {
        highestDevice = device.getId();
      }
    }

    return highestDevice + 1;
  }

  public int getEnabledDeviceCount() {
    int count = 0;

    for (Device device : devices) {
      if (device.isEnabled()) count++;
    }

    return count;
  }

  public boolean isRateLimited() {
    return true;
  }

  public Optional<String> getRelay() {
    return Optional.empty();
  }

  public void setIdentityKey(String identityKey) {
    this.identityKey = identityKey;
  }

  public String getIdentityKey() {
    return identityKey;
  }

  public long getLastSeen() {
    long lastSeen = 0;

    for (Device device : devices) {
      if (device.getLastSeen() > lastSeen) {
        lastSeen = device.getLastSeen();
      }
    }

    return lastSeen;
  }

  public String getProfileName() {
    return name;
  }

  public void setProfileName(String name) {
    this.name = name;
  }

  public String getAvatar() {
    return avatar;
  }

  public void setAvatar(String avatar) {
    this.avatar = avatar;
  }

  public String getAvatarDigest() {
    return avatarDigest;
  }

  public void setAvatarDigest(String avatarDigest) {
    this.avatarDigest = avatarDigest;
  }

  public void setPin(String pin) {
    this.pin = pin;
  }

  public void setRegistrationLock(String registrationLock, String registrationLockSalt) {
    this.registrationLock     = registrationLock;
    this.registrationLockSalt = registrationLockSalt;
  }

  public StoredRegistrationLock getRegistrationLock() {
    return new StoredRegistrationLock(Optional.ofNullable(registrationLock), Optional.ofNullable(registrationLockSalt), Optional.ofNullable(pin), getLastSeen());
  }
  
  public Optional<byte[]> getUnidentifiedAccessKey() {
    return Optional.ofNullable(unidentifiedAccessKey);
  }

  public void setUnidentifiedAccessKey(byte[] unidentifiedAccessKey) {
    this.unidentifiedAccessKey = unidentifiedAccessKey;
  }

  public boolean isUnrestrictedUnidentifiedAccess() {
    return unrestrictedUnidentifiedAccess;
  }

  public void setUnrestrictedUnidentifiedAccess(boolean unrestrictedUnidentifiedAccess) {
    this.unrestrictedUnidentifiedAccess = unrestrictedUnidentifiedAccess;
  }

  public boolean isFor(AmbiguousIdentifier identifier) {
    if      (identifier.hasUuid())   return identifier.getUuid().equals(uuid);
    else if (identifier.hasNumber()) return identifier.getNumber().equals(number);
    else                             throw new AssertionError();
  }

  // Principal implementation

  @Override
  @JsonIgnore
  public String getName() {
    return null;
  }

  @Override
  @JsonIgnore
  public boolean implies(Subject subject) {
    return false;
  }

}
