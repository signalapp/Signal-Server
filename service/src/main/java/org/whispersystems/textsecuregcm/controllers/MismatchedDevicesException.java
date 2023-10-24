/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import java.util.List;

public class MismatchedDevicesException extends Exception {

  private final List<Byte> missingDevices;
  private final List<Byte> extraDevices;

  public MismatchedDevicesException(List<Byte> missingDevices, List<Byte> extraDevices) {
    this.missingDevices = missingDevices;
    this.extraDevices   = extraDevices;
  }

  public List<Byte> getMissingDevices() {
    return missingDevices;
  }

  public List<Byte> getExtraDevices() {
    return extraDevices;
  }
}
