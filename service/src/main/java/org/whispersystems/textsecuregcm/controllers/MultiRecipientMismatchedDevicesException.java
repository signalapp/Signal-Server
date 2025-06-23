/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import java.util.Map;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;

public class MultiRecipientMismatchedDevicesException extends Exception {

  private final Map<ServiceIdentifier, MismatchedDevices> mismatchedDevicesByServiceIdentifier;

  public MultiRecipientMismatchedDevicesException(
      final Map<ServiceIdentifier, MismatchedDevices> mismatchedDevicesByServiceIdentifier) {

    if (mismatchedDevicesByServiceIdentifier.isEmpty()) {
      throw new IllegalArgumentException("Must provide non-empty map of service identifiers to mismatched devices");
    }

    this.mismatchedDevicesByServiceIdentifier = mismatchedDevicesByServiceIdentifier;
  }

  public Map<ServiceIdentifier, MismatchedDevices> getMismatchedDevicesByServiceIdentifier() {
    return mismatchedDevicesByServiceIdentifier;
  }
}
