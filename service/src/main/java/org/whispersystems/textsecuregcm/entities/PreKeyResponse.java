/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

public class PreKeyResponse {

  @JsonProperty
  @Schema(description="the public identity key for the requested identity")
  private String identityKey;

  @JsonProperty
  @Schema(description="information about each requested device")
  private List<PreKeyResponseItem> devices;

  public PreKeyResponse() {}

  public PreKeyResponse(String identityKey, List<PreKeyResponseItem> devices) {
    this.identityKey = identityKey;
    this.devices     = devices;
  }

  @VisibleForTesting
  public String getIdentityKey() {
    return identityKey;
  }

  @VisibleForTesting
  @JsonIgnore
  public PreKeyResponseItem getDevice(int deviceId) {
    for (PreKeyResponseItem device : devices) {
      if (device.getDeviceId() == deviceId) return device;
    }

    return null;
  }

  @VisibleForTesting
  @JsonIgnore
  public int getDevicesCount() {
    return devices.size();
  }

}
