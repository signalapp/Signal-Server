/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public class AccountAttributes {

  private static final String UAK_VALIDATION_COUNTER_NAME = MetricsUtil.name(AccountAttributes.class, "uakValidation");

  @JsonUnwrapped
  @Valid
  private DeviceAttributes deviceAttributes;

  @JsonProperty
  @ExactlySize({0, 64})
  private String registrationLock;

  @JsonProperty
  @ExactlySize({0, UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH})
  private byte[] unidentifiedAccessKey;

  @JsonProperty
  private boolean unrestrictedUnidentifiedAccess;

  @JsonProperty
  private boolean discoverableByPhoneNumber = true;

  @JsonProperty
  @Nullable
  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
  @ExactlySize({0, 32})
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
      final Set<DeviceCapability> capabilities) {

    this.deviceAttributes = new DeviceAttributes(fetchesMessages, registrationId, phoneNumberIdentifierRegistrationId, name, capabilities);
    this.registrationLock = registrationLock;
    this.discoverableByPhoneNumber = discoverableByPhoneNumber;
  }

  public boolean getFetchesMessages() {
    return deviceAttributes.fetchesMessages();
  }

  public int getRegistrationId() {
    return deviceAttributes.registrationId();
  }

  public int getPhoneNumberIdentityRegistrationId() {
    return deviceAttributes.phoneNumberIdentityRegistrationId();
  }

  public byte[] getName() {
    return deviceAttributes.name();
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

  @Nullable
  public Set<DeviceCapability> getCapabilities() {
    return deviceAttributes.capabilities();
  }

  public boolean isDiscoverableByPhoneNumber() {
    return discoverableByPhoneNumber;
  }

  public Optional<byte[]> recoveryPassword() {
    return Optional.ofNullable(recoveryPassword);
  }

  @VisibleForTesting
  public AccountAttributes setUnidentifiedAccessKey(final byte[] unidentifiedAccessKey) {
    this.unidentifiedAccessKey = unidentifiedAccessKey;
    return this;
  }

  @VisibleForTesting
  public AccountAttributes setRecoveryPassword(final byte[] recoveryPassword) {
    this.recoveryPassword = recoveryPassword;
    return this;
  }

  @VisibleForTesting
  public AccountAttributes setDeviceAttributes(final DeviceAttributes deviceAttributes) {
    this.deviceAttributes = deviceAttributes;
    return this;
  }

  @VisibleForTesting
  public AccountAttributes setUnrestrictedUnidentifiedAccess(final boolean unrestrictedUnidentifiedAccess) {
    this.unrestrictedUnidentifiedAccess = unrestrictedUnidentifiedAccess;
    return this;
  }

  @VisibleForTesting
  public static final boolean ENFORCE_VALID_UNRESTRICTED_UAK = false;

  @AssertTrue
  @Schema(hidden = true)
  public boolean isUnrestrictedUakValid() {

    final boolean valid =
        (unrestrictedUnidentifiedAccess && (unidentifiedAccessKey == null || unidentifiedAccessKey.length == 0))
            || (!unrestrictedUnidentifiedAccess && (unidentifiedAccessKey != null
            && unidentifiedAccessKey.length == 16));

    Metrics.counter(UAK_VALIDATION_COUNTER_NAME,
        "valid", String.valueOf(valid),
        "unrestricted", String.valueOf(unrestrictedUnidentifiedAccess)
    ).increment();

    // initially, only gather metrics
    return true;
  }
}
