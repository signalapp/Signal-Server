/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import java.util.Set;

public record MismatchedDevices(Set<Byte> missingDeviceIds, Set<Byte> extraDeviceIds, Set<Byte> staleDeviceIds) {

  public MismatchedDevices {
    if (missingDeviceIds.isEmpty() && extraDeviceIds.isEmpty() && staleDeviceIds.isEmpty()) {
      throw new IllegalArgumentException("At least one of missingDevices, extraDevices, or staleDevices must be non-empty");
    }
  }
}
