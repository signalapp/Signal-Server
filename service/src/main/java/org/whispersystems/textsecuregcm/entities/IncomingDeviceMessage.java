/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class IncomingDeviceMessage {
  private final int type;

  @Min(1)
  private final long deviceId;

  @Min(0)
  @Max(65536)
  private final int registrationId;

  @NotNull
  private final byte[] content;

  public IncomingDeviceMessage(int type, long deviceId, int registrationId, byte[] content) {
    this.type = type;
    this.deviceId = deviceId;
    this.registrationId = registrationId;
    this.content = content;
  }

  public int getType() {
    return type;
  }

  public long getDeviceId() {
    return deviceId;
  }

  public int getRegistrationId() {
    return registrationId;
  }

  public byte[] getContent() {
    return content;
  }
}
