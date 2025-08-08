/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;


import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.auth.StoredRegistrationLock;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64UrlAdapter;
import org.whispersystems.textsecuregcm.util.IdentityKeyAdapter;

@JsonFilter("Account")
public class Account {

  private static final Logger logger = LoggerFactory.getLogger(Account.class);

  @JsonProperty
  private UUID uuid;

  @JsonProperty("pni")
  private UUID phoneNumberIdentifier;

  @JsonProperty
  private String number;

  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64UrlAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64UrlAdapter.Deserializing.class)
  @Nullable
  private byte[] usernameHash;

  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64UrlAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64UrlAdapter.Deserializing.class)
  @Nullable
  private byte[] reservedUsernameHash;

  @JsonProperty
  @Nullable
  private UUID usernameLinkHandle;

  @JsonProperty("eu")
  @Nullable
  private byte[] encryptedUsername;

  @JsonProperty
  private List<Device> devices = new ArrayList<>();

  @JsonProperty
  @JsonSerialize(using = IdentityKeyAdapter.Serializer.class)
  @JsonDeserialize(using = IdentityKeyAdapter.Deserializer.class)
  private IdentityKey identityKey;

  @JsonProperty("pniIdentityKey")
  @JsonSerialize(using = IdentityKeyAdapter.Serializer.class)
  @JsonDeserialize(using = IdentityKeyAdapter.Deserializer.class)
  private IdentityKey phoneNumberIdentityKey;

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

  @JsonProperty("bcr")
  @Nullable
  private byte[] messagesBackupCredentialRequest;

  @JsonProperty("mbcr")
  @Nullable
  private byte[] mediaBackupCredentialRequest;

  @JsonProperty("bv")
  @Nullable
  private BackupVoucher backupVoucher;

  @JsonProperty
  private int version;

  @JsonProperty("holds")
  private List<UsernameHold> usernameHolds = Collections.emptyList();

  @JsonIgnore
  private boolean stale;

  public record UsernameHold(@JsonProperty("uh") byte[] usernameHash, @JsonProperty("e") long expirationSecs) {}

  public record BackupVoucher(@JsonProperty("rl") long receiptLevel, @JsonProperty("e") Instant expiration) {}

  public UUID getIdentifier(final IdentityType identityType) {
    return switch (identityType) {
      case ACI -> getUuid();
      case PNI -> getPhoneNumberIdentifier();
    };
  }

  public UUID getUuid() {
    // this is the one method that may be called on a stale account
    return uuid;
  }

  public void setUuid(final UUID uuid) {
    requireNotStale();

    this.uuid = uuid;
  }

  public UUID getPhoneNumberIdentifier() {
    requireNotStale();

    return phoneNumberIdentifier;
  }

  /**
   * Tests whether this account's account identifier or phone number identifier (depending on the given service
   * identifier's identity type) matches the given service identifier.
   *
   * @param serviceIdentifier the identifier to test
   * @return {@code true} if this account's identifier or phone number identifier matches
   */
  public boolean isIdentifiedBy(final ServiceIdentifier serviceIdentifier) {
    return switch (serviceIdentifier.identityType()) {
      case ACI -> serviceIdentifier.uuid().equals(uuid);
      case PNI -> serviceIdentifier.uuid().equals(phoneNumberIdentifier);
    };
  }

  public String getNumber() {
    requireNotStale();

    return number;
  }

  public void setNumber(final String number, final UUID phoneNumberIdentifier) {
    requireNotStale();

    this.number = number;
    this.phoneNumberIdentifier = phoneNumberIdentifier;
  }

  public Optional<byte[]> getUsernameHash() {
    requireNotStale();

    return Optional.ofNullable(usernameHash);
  }

  public void setUsernameHash(final byte[] usernameHash) {
    requireNotStale();

    this.usernameHash = usernameHash;
  }

  public Optional<byte[]> getReservedUsernameHash() {
    requireNotStale();

    return Optional.ofNullable(reservedUsernameHash);
  }

  public void setReservedUsernameHash(final byte[] reservedUsernameHash) {
    requireNotStale();

    this.reservedUsernameHash = reservedUsernameHash;
  }

  @Nullable
  public UUID getUsernameLinkHandle() {
    requireNotStale();
    return usernameLinkHandle;
  }

  public Optional<byte[]> getEncryptedUsername() {
    requireNotStale();
    return Optional.ofNullable(encryptedUsername);
  }

  public void setUsernameLinkDetails(@Nullable final UUID usernameLinkHandle, @Nullable final byte[] encryptedUsername) {
    requireNotStale();
    if ((usernameLinkHandle == null) ^ (encryptedUsername == null)) {
      throw new IllegalArgumentException("Both or neither arguments must be null");
    }
    if (usernameHash == null && encryptedUsername != null) {
      throw new IllegalArgumentException("usernameHash field must be set to store username link");
    }
    this.encryptedUsername = encryptedUsername;
    this.usernameLinkHandle = usernameLinkHandle;
  }

  /*
   * This method is intentionally left package-private so that it's only used
   * when Account is read from DB
   */
  void setUsernameLinkHandle(@Nullable final UUID usernameLinkHandle) {
    requireNotStale();
    this.usernameLinkHandle = usernameLinkHandle;
  }

  public void addDevice(final Device device) {
    requireNotStale();

    removeDevice(device.getId());
    this.devices.add(device);
  }

  public void removeDevice(final byte deviceId) {
    requireNotStale();

    this.devices.removeIf(device -> device.getId() == deviceId);
  }

  public List<Device> getDevices() {
    requireNotStale();

    return devices;
  }

  public Device getPrimaryDevice() {
    requireNotStale();

    return getDevice(Device.PRIMARY_ID)
        .orElseThrow(() -> new IllegalStateException("All accounts must have a primary device"));
  }

  public Optional<Device> getDevice(final byte deviceId) {
    requireNotStale();

    return devices.stream().filter(device -> device.getId() == deviceId).findFirst();
  }

  public boolean hasCapability(final DeviceCapability capability) {
    requireNotStale();

    return switch (capability.getAccountCapabilityMode()) {
      case PRIMARY_DEVICE -> getPrimaryDevice().hasCapability(capability);
      case ANY_DEVICE -> devices.stream().anyMatch(device -> device.hasCapability(capability));
      case ALL_DEVICES -> devices.stream().allMatch(device -> device.hasCapability(capability));
      case ALWAYS_CAPABLE -> true;
    };
  }

  public byte getNextDeviceId() {
    requireNotStale();

    byte candidateId = Device.PRIMARY_ID + 1;

    while (getDevice(candidateId).isPresent()) {
      candidateId++;
    }

    if (candidateId <= Device.PRIMARY_ID) {
      throw new RuntimeException("device ID overflow");
    }

    return candidateId;
  }

  public void setIdentityKey(final IdentityKey identityKey) {
    requireNotStale();

    this.identityKey = identityKey;
  }

  public IdentityKey getIdentityKey(final IdentityType identityType) {
    requireNotStale();

    return switch (identityType) {
      case ACI -> identityKey;
      case PNI -> phoneNumberIdentityKey;
    };
  }

  public void setPhoneNumberIdentityKey(final IdentityKey phoneNumberIdentityKey) {
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

  public void setCurrentProfileVersion(final String currentProfileVersion) {
    requireNotStale();

    this.currentProfileVersion = currentProfileVersion;
  }

  public List<AccountBadge> getBadges() {
    requireNotStale();

    return badges;
  }

  public void setBadges(final Clock clock, final List<AccountBadge> badges) {
    requireNotStale();

    this.badges = badges;

    purgeStaleBadges(clock);
  }

  public void addBadge(final Clock clock, final AccountBadge badge) {
    requireNotStale();
    boolean added = false;
    for (int i = 0; i < badges.size(); i++) {
      final AccountBadge badgeInList = badges.get(i);
      if (Objects.equals(badgeInList.id(), badge.id())) {
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

  public void makeBadgePrimaryIfExists(final Clock clock, final String badgeId) {
    requireNotStale();

    // early exit if it's already the first item in the list
    if (!badges.isEmpty() && Objects.equals(badges.get(0).id(), badgeId)) {
      purgeStaleBadges(clock);
      return;
    }

    int indexOfBadge = -1;
    for (int i = 1; i < badges.size(); i++) {
      if (Objects.equals(badgeId, badges.get(i).id())) {
        indexOfBadge = i;
        break;
      }
    }

    if (indexOfBadge != -1) {
      badges.add(0, badges.remove(indexOfBadge));
    }

    purgeStaleBadges(clock);
  }

  public void removeBadge(final Clock clock, final String id) {
    requireNotStale();

    badges.removeIf(accountBadge -> Objects.equals(accountBadge.id(), id));
    purgeStaleBadges(clock);
  }

  private void purgeStaleBadges(final Clock clock) {
    final Instant now = clock.instant();
    badges.removeIf(accountBadge -> now.isAfter(accountBadge.expiration()));
  }

  public void setRegistrationLockFromAttributes(final AccountAttributes attributes) {
    if (StringUtils.isNotEmpty(attributes.getRegistrationLock())) {
      final SaltedTokenHash credentials = SaltedTokenHash.generateFor(attributes.getRegistrationLock());
      setRegistrationLock(credentials.hash(), credentials.salt());
    } else {
      setRegistrationLock(null, null);
    }
  }

  public void setRegistrationLock(final String registrationLock, final String registrationLockSalt) {
    requireNotStale();

    this.registrationLock     = registrationLock;
    this.registrationLockSalt = registrationLockSalt;
  }

  public StoredRegistrationLock getRegistrationLock() {
    requireNotStale();

    return new StoredRegistrationLock(Optional.ofNullable(registrationLock), Optional.ofNullable(registrationLockSalt), Instant.ofEpochMilli(getLastSeen()));
  }

  public Optional<byte[]> getUnidentifiedAccessKey() {
    requireNotStale();

    return Optional.ofNullable(unidentifiedAccessKey);
  }

  public void setUnidentifiedAccessKey(final byte[] unidentifiedAccessKey) {
    requireNotStale();

    this.unidentifiedAccessKey = unidentifiedAccessKey;
  }

  public boolean isUnrestrictedUnidentifiedAccess() {
    requireNotStale();

    return unrestrictedUnidentifiedAccess;
  }

  public void setUnrestrictedUnidentifiedAccess(final boolean unrestrictedUnidentifiedAccess) {
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

  public int getVersion() {
    requireNotStale();

    return version;
  }

  public void setVersion(final int version) {
    requireNotStale();

    this.version = version;
  }

  public void setBackupCredentialRequests(final byte[] messagesBackupCredentialRequest,
      final byte[] mediaBackupCredentialRequest) {

    requireNotStale();

    this.messagesBackupCredentialRequest = messagesBackupCredentialRequest;
    this.mediaBackupCredentialRequest = mediaBackupCredentialRequest;
  }

  public Optional<byte[]> getBackupCredentialRequest(final BackupCredentialType credentialType) {
    requireNotStale();

    return Optional.ofNullable(switch (credentialType) {
      case MESSAGES -> messagesBackupCredentialRequest;
      case MEDIA -> mediaBackupCredentialRequest;
    });
  }

  public @Nullable BackupVoucher getBackupVoucher() {
    requireNotStale();

    return backupVoucher;
  }

  public void setBackupVoucher(final @Nullable BackupVoucher backupVoucher) {
    requireNotStale();

    this.backupVoucher = backupVoucher;
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
  public void lockAuthTokenHash() {
    devices.forEach(Device::lockAuthTokenHash);
  }

  public List<UsernameHold> getUsernameHolds() {
    return Collections.unmodifiableList(usernameHolds);
  }

  public void setUsernameHolds(final List<UsernameHold> usernameHolds) {
    this.requireNotStale();
    this.usernameHolds = usernameHolds;
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
