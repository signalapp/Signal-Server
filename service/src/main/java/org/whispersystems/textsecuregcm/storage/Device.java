/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.DeviceCapabilityAdapter;
import org.whispersystems.textsecuregcm.util.DeviceNameByteArrayAdapter;

public class Device {

  public static final byte PRIMARY_ID = 1;
  public static final byte MAXIMUM_DEVICE_ID = Byte.MAX_VALUE;
  public static final int MAX_REGISTRATION_ID = 0x3FFF;
  public static final List<Byte> ALL_POSSIBLE_DEVICE_IDS = IntStream.range(Device.PRIMARY_ID, MAXIMUM_DEVICE_ID).boxed()
      .map(Integer::byteValue).collect(Collectors.toList());

  private static final long ALLOWED_LINKED_IDLE_MILLIS = Duration.ofDays(45).toMillis();
  private static final long ALLOWED_PRIMARY_IDLE_MILLIS = Duration.ofDays(180).toMillis();

  @JsonDeserialize(using = DeviceIdDeserializer.class)
  @JsonProperty
  private byte id;

  @JsonProperty
  @JsonSerialize(using = DeviceNameByteArrayAdapter.Serializer.class)
  @JsonDeserialize(using = DeviceNameByteArrayAdapter.Deserializer.class)
  private byte[] name;

  @JsonProperty("createdAt")
  @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
  private byte[] createdAtCiphertext;

  @JsonProperty
  private String  authToken;

  @JsonProperty
  private String  salt;

  @JsonProperty
  private String  gcmId;

  @JsonProperty
  private String  apnId;

  @JsonProperty
  private long pushTimestamp;

  @JsonProperty
  private boolean fetchesMessages;

  @JsonProperty
  private int registrationId;

  @JsonProperty("pniRegistrationId")
  private int phoneNumberIdentityRegistrationId;

  @JsonProperty
  private long lastSeen;

  @JsonProperty
  private long created;

  @JsonProperty
  private String userAgent;

  @JsonProperty
  @JsonSerialize(using = DeviceCapabilityAdapter.Serializer.class)
  @JsonDeserialize(using = DeviceCapabilityAdapter.Deserializer.class)
  private Set<DeviceCapability> capabilities = Collections.emptySet();

  public String getApnId() {
    return apnId;
  }

  public void setApnId(String apnId) {
    this.apnId = apnId;

    if (apnId != null) {
      this.pushTimestamp = System.currentTimeMillis();
    }
  }

  public void setLastSeen(long lastSeen) {
    this.lastSeen = lastSeen;
  }

  public long getLastSeen() {
    return lastSeen;
  }

  public void setCreated(long created) {
    this.created = created;
  }

  public long getCreated() {
    return this.created;
  }

  public void setCreatedAtCiphertext(byte[] createdAtCiphertext) {
    this.createdAtCiphertext = createdAtCiphertext;
  }

  public byte[] getCreatedAtCiphertext() {
    return this.createdAtCiphertext;
  }

  public String getGcmId() {
    return gcmId;
  }

  public void setGcmId(String gcmId) {
    this.gcmId = gcmId;

    if (gcmId != null) {
      this.pushTimestamp = System.currentTimeMillis();
    }
  }

  public byte getId() {
    return id;
  }

  public void setId(byte id) {
    this.id = id;
  }

  public byte[] getName() {
    return name;
  }

  public void setName(byte[] name) {
    this.name = name;
  }

  public void setAuthTokenHash(SaltedTokenHash credentials) {
    this.authToken = credentials.hash();
    this.salt      = credentials.salt();
  }

  /**
   * Has this device been manually locked?
   *
   * We lock a device by prepending "!" to its token.
   * This character cannot normally appear in valid tokens.
   *
   * @return true if the credential was locked, false otherwise.
   */
  public boolean hasLockedCredentials() {
    SaltedTokenHash auth = getAuthTokenHash();
    return auth.hash().startsWith("!");
  }

  /**
   * Lock device by invalidating authentication tokens.
   *
   * This should only be used from Account::lockAuthenticationCredentials.
   *
   * See that method for more information.
   */
  public void lockAuthTokenHash() {
    SaltedTokenHash oldAuth = getAuthTokenHash();
    String token = "!" + oldAuth.hash();
    String salt = oldAuth.salt();
    setAuthTokenHash(new SaltedTokenHash(token, salt));
  }

  public SaltedTokenHash getAuthTokenHash() {
    return new SaltedTokenHash(authToken, salt);
  }

  @VisibleForTesting
  public Set<DeviceCapability> getCapabilities() {
    return capabilities;
  }

  @JsonSetter
  public void setCapabilities(@Nullable final Set<DeviceCapability> capabilities) {
    this.capabilities = (capabilities == null || capabilities.isEmpty())
        ? Collections.emptySet()
        : EnumSet.copyOf(capabilities);
  }

  public boolean hasCapability(final DeviceCapability capability) {
    return capabilities.contains(capability);
  }

  public boolean isExpired() {
    return isPrimary()
        ? lastSeen < (System.currentTimeMillis() - ALLOWED_PRIMARY_IDLE_MILLIS)
        : lastSeen < (System.currentTimeMillis() - ALLOWED_LINKED_IDLE_MILLIS);
  }

  public boolean getFetchesMessages() {
    return fetchesMessages;
  }

  public void setFetchesMessages(boolean fetchesMessages) {
    this.fetchesMessages = fetchesMessages;
  }

  public boolean isPrimary() {
    return getId() == PRIMARY_ID;
  }

  public int getRegistrationId(final IdentityType identityType) {
    return switch (identityType) {
      case ACI -> registrationId;
      case PNI -> phoneNumberIdentityRegistrationId;
    };
  }

  public void setRegistrationId(int registrationId) {
    this.registrationId = registrationId;
  }

  public void setPhoneNumberIdentityRegistrationId(final int phoneNumberIdentityRegistrationId) {
    this.phoneNumberIdentityRegistrationId = phoneNumberIdentityRegistrationId;
  }

  public long getPushTimestamp() {
    return pushTimestamp;
  }

  public void setUserAgent(String userAgent) {
    this.userAgent = userAgent;
  }

  public String getUserAgent() {
    return this.userAgent;
  }
}
