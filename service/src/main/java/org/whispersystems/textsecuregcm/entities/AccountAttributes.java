/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import static org.whispersystems.textsecuregcm.util.RegistrationIdValidator.validRegistrationId;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Size;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public class AccountAttributes {

  @JsonProperty
  private boolean fetchesMessages;

  @JsonProperty
  private int registrationId;

  @JsonProperty("pniRegistrationId")
  private int phoneNumberIdentityRegistrationId;

  @JsonProperty
  @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
  @Size(max = 225)
  private byte[] name;

  @JsonProperty
  private String registrationLock;

  @JsonProperty
  @ExactlySize({0, UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH})
  private byte[] unidentifiedAccessKey;

  @JsonProperty
  private boolean unrestrictedUnidentifiedAccess;

  @JsonProperty
  private DeviceCapabilities capabilities;

  @JsonProperty
  private boolean discoverableByPhoneNumber = true;

  @JsonProperty
  @Nullable
  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
  private byte[] recoveryPassword = null;

  public AccountAttributes() {
  }

  @VisibleForTesting
  public AccountAttributes(
      final boolean fetchesMessages,
      final int registrationId,
      final int phoneNumberIdentifierRegistrationId,
      final byte[] name,
      final String registrationLock,
      final boolean discoverableByPhoneNumber,
      final DeviceCapabilities capabilities) {
    this.fetchesMessages = fetchesMessages;
    this.registrationId = registrationId;
    this.phoneNumberIdentityRegistrationId = phoneNumberIdentifierRegistrationId;
    this.name = name;
    this.registrationLock = registrationLock;
    this.discoverableByPhoneNumber = discoverableByPhoneNumber;
    this.capabilities = capabilities;
  }

  public boolean getFetchesMessages() {
    return fetchesMessages;
  }

  public int getRegistrationId() {
    return registrationId;
  }

  public int getPhoneNumberIdentityRegistrationId() {
    return phoneNumberIdentityRegistrationId;
  }

  public byte[] getName() {
    return name;
  }

  public String getRegistrationLock() {
    return registrationLock;
  }

  public byte[] getUnidentifiedAccessKey() {
    return unidentifiedAccessKey;
  }

  public boolean isUnrestrictedUnidentifiedAccess() {
    return unrestrictedUnidentifiedAccess;
  }

  public DeviceCapabilities getCapabilities() {
    return capabilities;
  }

  public boolean isDiscoverableByPhoneNumber() {
    return discoverableByPhoneNumber;
  }

  public Optional<byte[]> recoveryPassword() {
    return Optional.ofNullable(recoveryPassword);
  }

  @VisibleForTesting
  public AccountAttributes withUnidentifiedAccessKey(final byte[] unidentifiedAccessKey) {
    this.unidentifiedAccessKey = unidentifiedAccessKey;
    return this;
  }

  @VisibleForTesting
  public AccountAttributes withRecoveryPassword(final byte[] recoveryPassword) {
    this.recoveryPassword = recoveryPassword;
    return this;
  }

  @VisibleForTesting
  public void setPhoneNumberIdentityRegistrationId(final Integer phoneNumberIdentityRegistrationId) {
    this.phoneNumberIdentityRegistrationId = phoneNumberIdentityRegistrationId;
  }

  @AssertTrue
  public boolean isEachRegistrationIdValid() {
    return validRegistrationId(registrationId) && validRegistrationId(phoneNumberIdentityRegistrationId);
  }
}
