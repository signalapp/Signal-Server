/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import javax.validation.constraints.Size;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;

public class AccountAttributes {

  @JsonProperty
  private boolean fetchesMessages;

  @JsonProperty
  private int registrationId;

  @JsonProperty
  @Size(max = 204, message = "This field must be less than 50 characters")
  private String name;

  @JsonProperty
  private String registrationLock;

  @JsonProperty
  private byte[] unidentifiedAccessKey;

  @JsonProperty
  private boolean unrestrictedUnidentifiedAccess;

  @JsonProperty
  private DeviceCapabilities capabilities;

  @JsonProperty
  private boolean discoverableByPhoneNumber = true;

  public AccountAttributes() {}

  @VisibleForTesting
  public AccountAttributes(boolean fetchesMessages, int registrationId, String name, String registrationLock,
      boolean discoverableByPhoneNumber, final DeviceCapabilities capabilities) {
    this.fetchesMessages = fetchesMessages;
    this.registrationId = registrationId;
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

  public String getName() {
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
}
