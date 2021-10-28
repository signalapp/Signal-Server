/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.StoredRegistrationLock;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.util.Util;
import javax.annotation.Nullable;

public class Account {

  @JsonIgnore
  private static final Logger logger = LoggerFactory.getLogger(Account.class);

  @JsonIgnore
  private UUID uuid;

  // Nullable only until initial migration is complete
  @JsonProperty("pni")
  @Nullable
  private UUID phoneNumberIdentifier;

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
  private List<AccountBadge> badges = new ArrayList<>();

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

  @JsonProperty
  private int version;

  @JsonIgnore
  private boolean stale;

  @JsonIgnore
  private boolean canonicallyDiscoverable;

  public Account() {}

  @VisibleForTesting
  public Account(String number, UUID uuid, final UUID phoneNumberIdentifier, Set<Device> devices, byte[] unidentifiedAccessKey) {
    this.number                = number;
    this.uuid                  = uuid;
    this.phoneNumberIdentifier = phoneNumberIdentifier;
    this.devices               = devices;
    this.unidentifiedAccessKey = unidentifiedAccessKey;
  }

  public UUID getUuid() {
    // this is the one method that may be called on a stale account
    return uuid;
  }

  public void setUuid(UUID uuid) {
    requireNotStale();

    this.uuid = uuid;
  }

  // Optional only until initial migration is complete
  public Optional<UUID> getPhoneNumberIdentifier() {
    requireNotStale();

    return Optional.ofNullable(phoneNumberIdentifier);
  }

  /**
   * Tests whether this account's account identifier or phone number identifier matches the given UUID.
   *
   * @param identifier the identifier to test
   * @return {@code true} if this account's identifier or phone number identifier matches
   */
  public boolean isIdentifiedBy(final UUID identifier) {
    return uuid.equals(identifier) || (phoneNumberIdentifier != null && phoneNumberIdentifier.equals(identifier));
  }

  public String getNumber() {
    requireNotStale();

    return number;
  }

  public void setNumber(String number, UUID phoneNumberIdentifier) {
    requireNotStale();

    this.number = number;
    this.phoneNumberIdentifier = phoneNumberIdentifier;
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

  public boolean isChangeNumberSupported() {
    requireNotStale();

    return devices.stream()
        .filter(Device::isEnabled)
        .allMatch(device -> device.getCapabilities() != null && device.getCapabilities().isChangeNumber());
  }

  public boolean isEnabled() {
    requireNotStale();

    return getMasterDevice().map(Device::isEnabled).orElse(false);
  }

  public long getNextDeviceId() {
    requireNotStale();

    long candidateId = Device.MASTER_ID + 1;

    while (getDevice(candidateId).isPresent()) {
      candidateId++;
    }

    return candidateId;
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

  public boolean isCanonicallyDiscoverable() {
    requireNotStale();

    return canonicallyDiscoverable;
  }

  public void setCanonicallyDiscoverable(boolean canonicallyDiscoverable) {
    requireNotStale();

    this.canonicallyDiscoverable = canonicallyDiscoverable;
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

  public List<AccountBadge> getBadges() {
    requireNotStale();

    return badges;
  }

  public void setBadges(Clock clock, List<AccountBadge> badges) {
    requireNotStale();

    this.badges = badges;

    purgeStaleBadges(clock);
  }

  public void addBadge(Clock clock, AccountBadge badge) {
    requireNotStale();

    boolean added = false;
    for (int i = 0; i < badges.size(); i++) {
      AccountBadge badgeInList = badges.get(i);
      if (Objects.equals(badgeInList.getId(), badge.getId())) {
        if (added) {
          badges.remove(i);
          i--;
        } else {
          badges.set(i, badgeInList.mergeWith(badge));
          added = true;
        }
      }
    }

    if (!added) {
      badges.add(badge);
    }

    purgeStaleBadges(clock);
  }

  public void makeBadgePrimaryIfExists(Clock clock, String badgeId) {
    requireNotStale();

    // early exit if it's already the first item in the list
    if (!badges.isEmpty() && Objects.equals(badges.get(0).getId(), badgeId)) {
      purgeStaleBadges(clock);
      return;
    }

    int indexOfBadge = -1;
    for (int i = 1; i < badges.size(); i++) {
      if (Objects.equals(badgeId, badges.get(i).getId())) {
        indexOfBadge = i;
        break;
      }
    }

    if (indexOfBadge != -1) {
      badges.add(0, badges.remove(indexOfBadge));
    }

    purgeStaleBadges(clock);
  }

  public void removeBadge(Clock clock, String id) {
    requireNotStale();

    badges.removeIf(accountBadge -> Objects.equals(accountBadge.getId(), id));
    purgeStaleBadges(clock);
  }

  private void purgeStaleBadges(Clock clock) {
    final Instant now = clock.instant();
    badges.removeIf(accountBadge -> now.isAfter(accountBadge.getExpiration()));
  }

  public void setRegistrationLockFromAttributes(final AccountAttributes attributes) {
    if (!Util.isEmpty(attributes.getRegistrationLock())) {
      AuthenticationCredentials credentials = new AuthenticationCredentials(attributes.getRegistrationLock());
      setRegistrationLock(credentials.getHashedAuthenticationToken(), credentials.getSalt());
    } else {
      setRegistrationLock(null, null);
    }
  }

  public void setRegistrationLock(String registrationLock, String registrationLockSalt) {
    requireNotStale();

    this.registrationLock     = registrationLock;
    this.registrationLockSalt = registrationLockSalt;
  }

  public StoredRegistrationLock getRegistrationLock() {
    requireNotStale();

    return new StoredRegistrationLock(Optional.ofNullable(registrationLock), Optional.ofNullable(registrationLockSalt), getLastSeen());
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

  public boolean isDiscoverableByPhoneNumber() {
    requireNotStale();

    return this.discoverableByPhoneNumber;
  }

  public void setDiscoverableByPhoneNumber(final boolean discoverableByPhoneNumber) {
    requireNotStale();

    this.discoverableByPhoneNumber = discoverableByPhoneNumber;
  }

  public boolean shouldBeVisibleInDirectory() {
    requireNotStale();

    return isEnabled() && isDiscoverableByPhoneNumber();
  }

  public int getVersion() {
    requireNotStale();

    return version;
  }

  public void setVersion(int version) {
    requireNotStale();

    this.version = version;
  }

  boolean isStale() {
    return stale;
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

}
