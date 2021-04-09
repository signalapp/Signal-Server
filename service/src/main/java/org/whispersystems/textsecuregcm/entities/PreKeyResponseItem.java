/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

public class PreKeyResponseItem {

  @JsonProperty
  private long deviceId;

  @JsonProperty
  private int registrationId;

  @JsonProperty
  private SignedPreKey signedPreKey;

  @JsonProperty
  private PreKey preKey;

  public PreKeyResponseItem() {}

  public PreKeyResponseItem(long deviceId, int registrationId, SignedPreKey signedPreKey, PreKey preKey) {
    this.deviceId       = deviceId;
    this.registrationId = registrationId;
    this.signedPreKey   = signedPreKey;
    this.preKey         = preKey;
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
  public int getRegistrationId() {
    return registrationId;
  }

  @VisibleForTesting
  public long getDeviceId() {
    return deviceId;
  }
}
