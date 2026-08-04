/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.zkgroup.ZkCredentialPublicKey;
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
import org.whispersystems.textsecuregcm.util.ZkCredentialPublicKeyAdapter;

@JsonFilter("Account")
public class Account {

  private static final Logger logger = LoggerFactory.getLogger(Account.class);

  @JsonProperty
  private UUID uuid;

  @JsonProperty("pni")
  @Nullable
  private UUID phoneNumberIdentifier;

  @JsonProperty
  @Nullable
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
  @Nullable
  private IdentityKey phoneNumberIdentityKey;

  @JsonProperty("cpv")
  @JsonDeserialize(using = ProfileVersionAdapter.Deserializing.class)
  private byte[] currentProfileVersion;

  @JsonProperty
  private List<AccountBadge> badges = new ArrayList<>();

  @JsonProperty
  private String registrationLock;

  @JsonProperty
  private String registrationLockSalt;

  @JsonProperty("uak")
  @Nullable
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

  @JsonProperty("zck")
  @Nullable
  @JsonSerialize(using = ZkCredentialPublicKeyAdapter.Serializer.class)
  @JsonDeserialize(using = ZkCredentialPublicKeyAdapter.Deserializer.class)
  private ZkCredentialPublicKey zkCredentialKey;

  @JsonProperty("zckr")
  @Nullable
  private Long zkCredentialKeyRotationId;

  @JsonProperty
  private int version;

  @JsonProperty("holds")
  private List<UsernameHold> usernameHolds = Collections.emptyList();

  @JsonProperty("arps")
  @Nullable
  private String accountRecoveryPasswordSalt;

  @JsonProperty("arph")
  @Nullable
  private String accountRecoveryPasswordHash;

  @JsonIgnore
  private boolean stale;

  public record UsernameHold(@JsonProperty("uh") byte[] usernameHash, @JsonProperty("e") long expirationSecs) {}

  public record BackupVoucher(@JsonProperty("rl") long receiptLevel, @JsonProperty("e") Instant expiration) {}

  /// Returns an identifier for the given identity type for this account with the assumption that all accounts have
  /// identifiers for all identity types.
  ///
  /// @param identityType the identity type for which to retrieve an account identifier
  ///
  /// @return the identifier for the given identity type
  ///
  /// @throws NoSuchElementException if the account does not have an identifier for the given identity type
  ///
  /// @deprecated Different identity types have significantly differing presence and staleness requirements/guarantees
  /// for their respective account identifiers. Please use [#getAccountIdentifier()] or
  /// [#getPhoneNumberIdentifierOptional()] instead.
  @Deprecated
  public UUID getIdentifier(final IdentityType identityType) {
    return switch (identityType) {
      case ACI -> getAccountIdentifier();
      case PNI -> getPhoneNumberIdentifier();
    };
  }

  /// Returns the core account identifier (ACI) for this account. An account's core identifier never changes.
  ///
  /// @return the core account identifier for this account
  public UUID getAccountIdentifier() {
    // this is the one method that may be called on a stale account
    return uuid;
  }

  public void setAccountIdentifier(final UUID accountIdentifier) {
    requireNotStale();

    this.uuid = accountIdentifier;
  }

  /// Returns the phone number identifier for this account.
  ///
  /// @throws NoSuchElementException if this account does not have a phone number identifier
  ///
  /// @return the phone number identifier for this account
  ///
  /// @deprecated Please use [#getPhoneNumberIdentifierOptional()] (which has clearer presence semantics) instead.
  @Deprecated
  public UUID getPhoneNumberIdentifier() {
    requireNotStale();

    if (phoneNumberIdentifier == null) {
      throw new NoSuchElementException();
    }

    return phoneNumberIdentifier;
  }

  /// Returns the phone number identifier for this account or empty if this account does not have a phone number.
  ///
  /// @return the phone number identifier for this account or empty if this account does not have a phone number
  public Optional<UUID> getPhoneNumberIdentifierOptional() {
    requireNotStale();

    return Optional.ofNullable(phoneNumberIdentifier);
  }

  /// Tests whether this account's account identifier or phone number identifier (depending on the given service
  /// identifier's identity type) matches the given service identifier.
  ///
  /// @param serviceIdentifier the identifier to test
  /// @return `true` if this account's identifier or phone number identifier matches
  public boolean isIdentifiedBy(final ServiceIdentifier serviceIdentifier) {
    return switch (serviceIdentifier.identityType()) {
      case ACI -> serviceIdentifier.uuid().equals(uuid);
      case PNI -> serviceIdentifier.uuid().equals(phoneNumberIdentifier);
    };
  }

  /// Returns the E.164-formatted phone number for this account.
  ///
  /// @return the E.164-formatted phone number for this account
  ///
  /// @throws NoSuchElementException if this account does not have a phone number
  @Deprecated
  public String getNumber() {
    requireNotStale();

    if (number == null) {
      throw new NoSuchElementException();
    }

    return number;
  }

  /// Returns the phone number for this account or empty if this account does not have a phone number.
  ///
  /// @return the phone number for this account or empty if this account does not have a phone number
  public Optional<String> getNumberOptional() {
    requireNotStale();

    return Optional.ofNullable(number);
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

  /// Returns an identity key for the given identity type for this account with the assumption that all accounts have
  /// identity keys for all identity types.
  ///
  /// @param identityType the identity type for which to retrieve an identity key
  ///
  /// @return the identity key for the given identity type
  ///
  /// @throws NoSuchElementException if the account does not have an identifier (and therefore identity key) for the given identity type
  ///
  /// @deprecated Different identity types have significantly differing existence requirements/guarantees
  /// for their respective identity keys. Please use [#getAccountIdentityKey()] or
  /// [#getPhoneNumberIdentityKey()] instead.
  @Deprecated
  public IdentityKey getIdentityKey(final IdentityType identityType) {
    requireNotStale();

    return switch (identityType) {
      case ACI -> identityKey;
      case PNI -> Optional.ofNullable(phoneNumberIdentityKey).orElseThrow(NoSuchElementException::new);
    };
  }

  /// Returns an identity key for the ACI identity for this account.
  public IdentityKey getAccountIdentityKey() {
    requireNotStale();
    return identityKey;
  }

  /// Returns an identity key for the phone-number identity for this account, if it has such an identity.
  ///
  /// @return the identity key for the PNI identity for the account if it has one, or an empty `Optional` otherwise.
  public Optional<IdentityKey> getPhoneNumberIdentityKey() {
    requireNotStale();
    return Optional.ofNullable(phoneNumberIdentityKey);
  }

  /// Sets the identity key for the phone-number identity of this account.
  ///
  /// @throws IllegalStateException if the account does not have a phone number identifier.
  public void setPhoneNumberIdentityKey(final IdentityKey phoneNumberIdentityKey) {
    requireNotStale();

    if (this.phoneNumberIdentifier == null) {
      throw new IllegalStateException();
    }
    this.phoneNumberIdentityKey = phoneNumberIdentityKey;
  }

  public long getLastSeen() {
    requireNotStale();
    return devices.stream()
        .map(Device::getLastSeen)
        .max(Long::compare)
        .orElse(0L);
  }

  public Optional<byte[]> getCurrentProfileVersion() {
    requireNotStale();

    return Optional.ofNullable(currentProfileVersion);
  }

  public void setCurrentProfileVersion(final byte[] currentProfileVersion) {
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

    if (number == null) {
      throw new IllegalArgumentException("Cannot set registration lock on account with no phone number");
    }

    this.registrationLock     = registrationLock;
    this.registrationLockSalt = registrationLockSalt;
  }

  public StoredRegistrationLock getRegistrationLock() {
    requireNotStale();

    return new StoredRegistrationLock(Optional.ofNullable(registrationLock), Optional.ofNullable(registrationLockSalt), Instant.ofEpochMilli(getLastSeen()));
  }

  public Optional<byte[]> getUnidentifiedAccessKey() {
    requireNotStale();

    return Optional.ofNullable((unidentifiedAccessKey == null || unidentifiedAccessKey.length == 0)
        ? null
        : unidentifiedAccessKey);
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

  /// Indicates whether this account may be discovered by its phone number via the contact discovery system (CDS).
  ///
  /// @return `true` if this account has a phone number and has opted into discovery by phone number or `false`
  /// otherwise
  ///
  /// @see #getPhoneNumberIdentifierOptional()
  /// @see #setDiscoverableByPhoneNumber(boolean)
  public boolean isDiscoverableByPhoneNumber() {
    requireNotStale();

    return getPhoneNumberIdentifierOptional().isPresent() && this.discoverableByPhoneNumber;
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

  /// Have all this account's devices been manually locked?
  ///
  /// @see Device#hasLockedCredentials
  ///
  /// @return true if all the account's devices were locked, false otherwise.
  public boolean hasLockedCredentials() {
    return devices.stream().allMatch(Device::hasLockedCredentials);
  }

  /// Lock account by invalidating authentication tokens.
  ///
  /// We only want to do this in cases where there is a potential conflict between the
  /// phone number holder and the registration lock holder. In that case, locking the
  /// account will ensure that either the registration lock holder proves ownership
  /// of the phone number, or after 7 days the phone number holder can register a new
  /// account.
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

  public Optional<ZkCredentialPublicKey> getZkCredentialKey() {
    return Optional.ofNullable(zkCredentialKey);
  }

  public void setZkCredentialKey(@Nullable final ZkCredentialPublicKey zkCredentialKey) {
    this.zkCredentialKey = zkCredentialKey;
  }

  @Nullable
  public Long getZkCredentialKeyRotationId() {
    return zkCredentialKeyRotationId;
  }

  public void setZkCredentialKeyRotationId(@Nullable final Long zkCredentialKeyRotationId) {
    this.zkCredentialKeyRotationId = zkCredentialKeyRotationId;
  }

  public Optional<SaltedTokenHash> getAccountRecoveryPassword() {
    requireNotStale();

    return accountRecoveryPasswordSalt != null && accountRecoveryPasswordHash != null
        ? Optional.of(new SaltedTokenHash(accountRecoveryPasswordSalt, accountRecoveryPasswordHash))
        : Optional.empty();
  }

  public void setAccountRecoveryPassword(final byte[] accountRecoveryPassword) {
    requireNotStale();

    setAccountRecoveryPassword(SaltedTokenHash.generateFor(HexFormat.of().formatHex(accountRecoveryPassword)));
  }

  public void setAccountRecoveryPassword(final SaltedTokenHash saltedAccountRecoveryPasswordHash) {
    requireNotStale();

    this.accountRecoveryPasswordSalt = saltedAccountRecoveryPasswordHash.salt();
    this.accountRecoveryPasswordHash = saltedAccountRecoveryPasswordHash.hash();
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

  private static class ProfileVersionAdapter {
    private static class Deserializing extends JsonDeserializer<byte[]> {
      private static final String CURRENT_PROFILE_VERSION_FORMAT_COUNTER_NAME = name(Account.class, "currentProfileMetricDeserialized");

      @Override
      public byte[] deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        final String val = jsonParser.getValueAsString();
        Metrics.counter(CURRENT_PROFILE_VERSION_FORMAT_COUNTER_NAME, Tags.of("format", val.length() == 64 ? "hex" : "base64")).increment();
        if (val.length() == 64) {
          return HexFormat.of().parseHex(val);
        } else {
          return Base64.getDecoder().decode(val);
        }
      }
    }
  }
}
