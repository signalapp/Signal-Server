/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

public class DeviceResponse {

  @JsonProperty
  private long deviceId;

  @VisibleForTesting
  public DeviceResponse() {}

  public DeviceResponse(long deviceId) {
    this.deviceId = deviceId;
  }

  public long getDeviceId() {
    return deviceId;
  }
}
