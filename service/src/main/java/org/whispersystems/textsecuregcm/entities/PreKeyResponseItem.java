/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import io.swagger.v3.oas.annotations.media.Schema;

public class PreKeyResponseItem {

  @JsonProperty
  @Schema(description="the device ID of the device to which this item pertains")
  private long deviceId;

  @JsonProperty
  @Schema(description="the registration ID for the device")
  private int registrationId;

  @JsonProperty
  @Schema(description="the signed elliptic-curve prekey for the device, if one has been set")
  private SignedPreKey signedPreKey;

  @JsonProperty
  @Schema(description="an unsigned elliptic-curve prekey for the device, if any remain")
  private PreKey preKey;

  @JsonProperty
  @Schema(description="a signed post-quantum prekey for the device " +
      "(a one-time prekey if any remain, otherwise the last-resort prekey if one has been set)")
  private SignedPreKey pqPreKey;

  public PreKeyResponseItem() {}

  public PreKeyResponseItem(long deviceId, int registrationId, SignedPreKey signedPreKey, PreKey preKey, SignedPreKey pqPreKey) {
    this.deviceId = deviceId;
    this.registrationId = registrationId;
    this.signedPreKey = signedPreKey;
    this.preKey = preKey;
    this.pqPreKey = pqPreKey;
  }

  @VisibleForTesting
  public SignedPreKey getSignedPreKey() {
    return signedPreKey;
  }

  @VisibleForTesting
  public PreKey getPreKey() {
    return preKey;
  }

  @VisibleForTesting
  public SignedPreKey getPqPreKey() {
    return pqPreKey;
  }

  @VisibleForTesting
  public int getRegistrationId() {
    return registrationId;
  }

  @VisibleForTesting
  public long getDeviceId() {
    return deviceId;
  }
}
