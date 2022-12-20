/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.StoredRegistrationLock;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.util.Util;

public class Account {

  @JsonIgnore
  private static final Logger logger = LoggerFactory.getLogger(Account.class);

  @JsonIgnore
  private UUID uuid;

  @JsonProperty("pni")
  private UUID phoneNumberIdentifier;

  @JsonProperty
  private String number;

  @JsonProperty
  @Nullable
  private String username;

  @JsonProperty
  @Nullable
  private byte[] reservedUsernameHash;

  @JsonProperty
  private List<Device> devices = new ArrayList<>();

  @JsonProperty
  private String identityKey;

  @JsonProperty("pniIdentityKey")
  private String phoneNumberIdentityKey;

  @JsonProperty("cpv")
  private String currentProfileVersion;

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


  public UUID getUuid() {
    // this is the one method that may be called on a stale account
    return uuid;
  }

  public void setUuid(UUID uuid) {
    requireNotStale();

    this.uuid = uuid;
  }

  public UUID getPhoneNumberIdentifier() {
    requireNotStale();

    return phoneNumberIdentifier;
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

  public Optional<String> getUsername() {
    requireNotStale();

    return Optional.ofNullable(username);
  }

  public void setUsername(final String username) {
    requireNotStale();

    this.username = username;
  }

  public Optional<byte[]> getReservedUsernameHash() {
    requireNotStale();

    return Optional.ofNullable(reservedUsernameHash);
  }

  public void setReservedUsernameHash(final byte[] reservedUsernameHash) {
    requireNotStale();

    this.reservedUsernameHash = reservedUsernameHash;
  }

  public void addDevice(Device device) {
    requireNotStale();

    removeDevice(device.getId());
    this.devices.add(device);
  }

  public void removeDevice(long deviceId) {
    requireNotStale();

    this.devices.removeIf(device -> device.getId() == deviceId);
  }

  public List<Device> getDevices() {
    requireNotStale();

    return devices;
  }

  public Optional<Device> getMasterDevice() {
    requireNotStale();

    return getDevice(Device.MASTER_ID);
  }

  public Optional<Device> getDevice(long deviceId) {
    requireNotStale();

    return devices.stream().filter(device -> device.getId() == deviceId).findFirst();
  }

  public boolean isStorageSupported() {
    requireNotStale();

    return devices.stream().anyMatch(device -> device.getCapabilities() != null && device.getCapabilities().isStorage());
  }

  public boolean isTransferSupported() {
    requireNotStale();

    return getMasterDevice().map(Device::getCapabilities).map(Device.DeviceCapabilities::isTransfer).orElse(false);
  }

  public boolean isSenderKeySupported() {
    return allEnabledDevicesHaveCapability(DeviceCapabilities::isSenderKey);
  }

  public boolean isAnnouncementGroupSupported() {
    return allEnabledDevicesHaveCapability(DeviceCapabilities::isAnnouncementGroup);
  }

  public boolean isChangeNumberSupported() {
    return allEnabledDevicesHaveCapability(DeviceCapabilities::isChangeNumber);
  }

  public boolean isPniSupported() {
    return allEnabledDevicesHaveCapability(DeviceCapabilities::isPni);
  }

  public boolean isStoriesSupported() {
    requireNotStale();

    return devices.stream()
        .filter(Device::isEnabled)
        .allMatch(device -> device.getCapabilities() != null && device.getCapabilities().isStories());
  }

  public boolean isGiftBadgesSupported() {
    return allEnabledDevicesHaveCapability(DeviceCapabilities::isGiftBadges);
  }

  public boolean isPaymentActivationSupported() {
    return allEnabledDevicesHaveCapability(DeviceCapabilities::isPaymentActivation);
  }

  private boolean allEnabledDevicesHaveCapability(Predicate<DeviceCapabilities> predicate) {
    requireNotStale();

    return devices.stream()
        .filter(Device::isEnabled)
        .allMatch(device -> device.getCapabilities() != null && predicate.test(device.getCapabilities()));
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

  public boolean isCanonicallyDiscoverable() {
    requireNotStale();

    return canonicallyDiscoverable;
  }

  public void setCanonicallyDiscoverable(boolean canonicallyDiscoverable) {
    requireNotStale();

    this.canonicallyDiscoverable = canonicallyDiscoverable;
  }

  public void setIdentityKey(String identityKey) {
    requireNotStale();

    this.identityKey = identityKey;
  }

  public String getIdentityKey() {
    requireNotStale();

    return identityKey;
  }

  public String getPhoneNumberIdentityKey() {
    return phoneNumberIdentityKey;
  }

  public void setPhoneNumberIdentityKey(final String phoneNumberIdentityKey) {
    this.phoneNumberIdentityKey = phoneNumberIdentityKey;
  }

  public long getLastSeen() {
    requireNotStale();
    return devices.stream()
        .map(Device::getLastSeen)
        .max(Long::compare)
        .orElse(0L);
  }

  public Optional<String> getCurrentProfileVersion() {
    requireNotStale();

    return Optional.ofNullable(currentProfileVersion);
  }

  public void setCurrentProfileVersion(String currentProfileVersion) {
    requireNotStale();

    this.currentProfileVersion = currentProfileVersion;
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


  /**
   * Have all this account's devices been manually locked?
   *
   * @see Device#hasLockedCredentials
   *
   * @return true if all the account's devices were locked, false otherwise.
   */
  public boolean hasLockedCredentials() {
    return devices.stream().allMatch(Device::hasLockedCredentials);
  }

  /**
   * Lock account by invalidating authentication tokens.
   *
   * We only want to do this in cases where there is a potential conflict between the
   * phone number holder and the registration lock holder. In that case, locking the
   * account will ensure that either the registration lock holder proves ownership
   * of the phone number, or after 7 days the phone number holder can register a new
   * account.
   */
  public void lockAuthenticationCredentials() {
    devices.forEach(Device::lockAuthenticationCredentials);
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
