/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.hibernate.validator.constraints.Length;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.storage.PaymentAddress;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

public class AccountAttributes {

  @JsonProperty
  private String signalingKey;

  @JsonProperty
  private boolean fetchesMessages;

  @JsonProperty
  private int registrationId;

  @JsonProperty
  @Length(max = 204, message = "This field must be less than 50 characters")
  private String name;

  @JsonProperty
  private String pin;

  @JsonProperty
  private String registrationLock;

  @JsonProperty
  private byte[] unidentifiedAccessKey;

  @JsonProperty
  private boolean unrestrictedUnidentifiedAccess;

  @JsonProperty
  private List<PaymentAddress> payments;

  @JsonProperty
  private DeviceCapabilities capabilities;

  @JsonProperty
  private boolean discoverableByPhoneNumber = true;

  public AccountAttributes() {}

  @VisibleForTesting
  public AccountAttributes(String signalingKey, boolean fetchesMessages, int registrationId, String pin) {
    this(signalingKey, fetchesMessages, registrationId, null, pin, null, null, true, null);
  }

  @VisibleForTesting
  public AccountAttributes(String signalingKey, boolean fetchesMessages, int registrationId, String name, String pin, String registrationLock, List<PaymentAddress> payments, boolean discoverableByPhoneNumber, final DeviceCapabilities capabilities) {
    this.signalingKey              = signalingKey;
    this.fetchesMessages           = fetchesMessages;
    this.registrationId            = registrationId;
    this.name                      = name;
    this.pin                       = pin;
    this.registrationLock          = registrationLock;
    this.payments                  = payments;
    this.discoverableByPhoneNumber = discoverableByPhoneNumber;
    this.capabilities              = capabilities;
  }

  public String getSignalingKey() {
    return signalingKey;
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

  public String getPin() {
    return pin;
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

  public List<PaymentAddress> getPayments() {
    return payments;
  }

  public boolean isDiscoverableByPhoneNumber() {
    return discoverableByPhoneNumber;
  }
}
