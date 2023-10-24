/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

import java.util.UUID;

public class DeviceResponse {
  @JsonProperty
  private UUID uuid;

  @JsonProperty
  private UUID pni;

  @JsonProperty
  private byte deviceId;

  @VisibleForTesting
  public DeviceResponse() {}

  public DeviceResponse(UUID uuid, UUID pni, byte deviceId) {
    this.uuid = uuid;
    this.pni = pni;
    this.deviceId = deviceId;
  }

  public UUID getUuid() {
    return uuid;
  }

  public UUID getPni() {
    return pni;
  }

  public byte getDeviceId() {
    return deviceId;
  }
}
