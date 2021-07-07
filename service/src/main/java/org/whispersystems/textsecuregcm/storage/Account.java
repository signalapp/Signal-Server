/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.security.Principal;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.security.auth.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AmbiguousIdentifier;
import org.whispersystems.textsecuregcm.auth.StoredRegistrationLock;

public class Account implements Principal  {

  @JsonIgnore
  private static final Logger logger = LoggerFactory.getLogger(Account.class);

  @JsonIgnore
  private UUID uuid;

  @JsonProperty
  private String number;

  @JsonProperty
  private Set<Device> devices = new HashSet<>();

  @JsonProperty
  private String identityKey;

  @JsonProperty("cpv")
  private String currentProfileVersion;

  @JsonProperty
  private String name;

  @JsonProperty
  private String avatar;

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

  @JsonProperty("inCds")
  private boolean discoverableByPhoneNumber = true;

  @JsonIgnore
  private Device authenticatedDevice;

  @JsonProperty
  private int version;

  @JsonIgnore
  private boolean stale;

  public Account() {}

  @VisibleForTesting
  public Account(String number, UUID uuid, Set<Device> devices, byte[] unidentifiedAccessKey) {
    this.number                = number;
    this.uuid                  = uuid;
    this.devices               = devices;
    this.unidentifiedAccessKey = unidentifiedAccessKey;
  }

  public Optional<Device> getAuthenticatedDevice() {
    requireNotStale();

    return Optional.ofNullable(authenticatedDevice);
  }

  public void setAuthenticatedDevice(Device device) {
    requireNotStale();

    this.authenticatedDevice = device;
  }

  public UUID getUuid() {
    // this is the one method that may be called on a stale account
    return uuid;
  }

  public void setUuid(UUID uuid) {
    requireNotStale();

    this.uuid = uuid;
  }

  public void setNumber(String number) {
    requireNotStale();

    this.number = number;
  }

  public String getNumber() {
    requireNotStale();

    return number;
  }

  public void addDevice(Device device) {
    requireNotStale();

    this.devices.remove(device);
    this.devices.add(device);
  }

  public void removeDevice(long deviceId) {
    requireNotStale();

    this.devices.remove(new Device(deviceId, null, null, null, null, null, null, false, 0, null, 0, 0, "NA", 0, null));
  }

  public Set<Device> getDevices() {
    requireNotStale();

    return devices;
  }

  public Optional<Device> getMasterDevice() {
    requireNotStale();

    return getDevice(Device.MASTER_ID);
  }

  public Optional<Device> getDevice(long deviceId) {
    requireNotStale();

    for (Device device : devices) {
      if (device.getId() == deviceId) {
        return Optional.of(device);
      }
    }

    return Optional.empty();
  }

  public boolean isGroupsV2Supported() {
    requireNotStale();

    return devices.stream()
                  .filter(Device::isEnabled)
                  .allMatch(Device::isGroupsV2Supported);
  }

  public boolean isStorageSupported() {
    requireNotStale();

    return devices.stream().anyMatch(device -> device.getCapabilities() != null && device.getCapabilities().isStorage());
  }

  public boolean isTransferSupported() {
    requireNotStale();

    return getMasterDevice().map(Device::getCapabilities).map(Device.DeviceCapabilities::isTransfer).orElse(false);
  }

  public boolean isGv1MigrationSupported() {
    requireNotStale();

    return devices.stream()
                  .filter(Device::isEnabled)
                  .allMatch(device -> device.getCapabilities() != null && device.getCapabilities().isGv1Migration());
  }

  public boolean isSenderKeySupported() {
    requireNotStale();

    return devices.stream()
        .filter(Device::isEnabled)
        .allMatch(device -> device.getCapabilities() != null && device.getCapabilities().isSenderKey());
  }

  public boolean isAnnouncementGroupSupported() {
    requireNotStale();

    return devices.stream()
        .filter(Device::isEnabled)
        .allMatch(device -> device.getCapabilities() != null && device.getCapabilities().isAnnouncementGroup());
  }

  public boolean isEnabled() {
    requireNotStale();

    return getMasterDevice().map(Device::isEnabled).orElse(false);
  }

  public long getNextDeviceId() {
    requireNotStale();

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
    requireNotStale();

    int count = 0;

    for (Device device : devices) {
      if (device.isEnabled()) count++;
    }

    return count;
  }

  public boolean isRateLimited() {
    requireNotStale();

    return true;
  }

  public Optional<String> getRelay() {
    requireNotStale();

    return Optional.empty();
  }

  public void setIdentityKey(String identityKey) {
    requireNotStale();

    this.identityKey = identityKey;
  }

  public String getIdentityKey() {
    requireNotStale();

    return identityKey;
  }

  public long getLastSeen() {
    requireNotStale();

    long lastSeen = 0;

    for (Device device : devices) {
      if (device.getLastSeen() > lastSeen) {
        lastSeen = device.getLastSeen();
      }
    }

    return lastSeen;
  }

  public Optional<String> getCurrentProfileVersion() {
    requireNotStale();

    return Optional.ofNullable(currentProfileVersion);
  }

  public void setCurrentProfileVersion(String currentProfileVersion) {
    requireNotStale();

    this.currentProfileVersion = currentProfileVersion;
  }

  public String getProfileName() {
    requireNotStale();

    return name;
  }

  public void setProfileName(String name) {
    requireNotStale();

    this.name = name;
  }

  public String getAvatar() {
    requireNotStale();

    return avatar;
  }

  public void setAvatar(String avatar) {
    requireNotStale();

    this.avatar = avatar;
  }

  public void setPin(String pin) {
    requireNotStale();

    this.pin = pin;
  }

  public void setRegistrationLock(String registrationLock, String registrationLockSalt) {
    requireNotStale();

    this.registrationLock     = registrationLock;
    this.registrationLockSalt = registrationLockSalt;
  }

  public StoredRegistrationLock getRegistrationLock() {
    requireNotStale();

    return new StoredRegistrationLock(Optional.ofNullable(registrationLock), Optional.ofNullable(registrationLockSalt), Optional.ofNullable(pin), getLastSeen());
  }
  
  public Optional<byte[]> getUnidentifiedAccessKey() {
    requireNotStale();

    return Optional.ofNullable(unidentifiedAccessKey);
  }

  public void setUnidentifiedAccessKey(byte[] unidentifiedAccessKey) {
    requireNotStale();

    this.unidentifiedAccessKey = unidentifiedAccessKey;
  }

  public boolean isUnrestrictedUnidentifiedAccess() {
    requireNotStale();

    return unrestrictedUnidentifiedAccess;
  }

  public void setUnrestrictedUnidentifiedAccess(boolean unrestrictedUnidentifiedAccess) {
    requireNotStale();

    this.unrestrictedUnidentifiedAccess = unrestrictedUnidentifiedAccess;
  }

  public boolean isFor(AmbiguousIdentifier identifier) {
    requireNotStale();

    if      (identifier.hasUuid())   return identifier.getUuid().equals(uuid);
    else if (identifier.hasNumber()) return identifier.getNumber().equals(number);
    else                             throw new AssertionError();
  }

  public boolean isDiscoverableByPhoneNumber() {
    requireNotStale();

    return this.discoverableByPhoneNumber;
  }

  public void setDiscoverableByPhoneNumber(final boolean discoverableByPhoneNumber) {
    requireNotStale();

    this.discoverableByPhoneNumber = discoverableByPhoneNumber;
  }

  public int getVersion() {
    requireNotStale();

    return version;
  }

  public void setVersion(int version) {
    requireNotStale();

    this.version = version;
  }

  public void markStale() {
    stale = true;
  }

  private void requireNotStale() {
    assert !stale;

    //noinspection ConstantConditions
    if (stale) {
      logger.error("Accessor called on stale account", new RuntimeException());
    }
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
