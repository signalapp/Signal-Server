/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import java.util.Map;

public class MultiRecipientMismatchedDevicesException extends Exception {

  private final Map<ServiceIdentifier, MismatchedDevices> mismatchedDevicesByServiceIdentifier;

  public MultiRecipientMismatchedDevicesException(
      final Map<ServiceIdentifier, MismatchedDevices> mismatchedDevicesByServiceIdentifier) {

    this.mismatchedDevicesByServiceIdentifier = mismatchedDevicesByServiceIdentifier;
  }

  public Map<ServiceIdentifier, MismatchedDevices> getMismatchedDevicesByServiceIdentifier() {
    return mismatchedDevicesByServiceIdentifier;
  }
}
