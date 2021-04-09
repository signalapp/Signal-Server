/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import java.util.List;

public class MismatchedDevicesException extends Exception {

  private final List<Long> missingDevices;
  private final List<Long> extraDevices;

  public MismatchedDevicesException(List<Long> missingDevices, List<Long> extraDevices) {
    this.missingDevices = missingDevices;
    this.extraDevices   = extraDevices;
  }

  public List<Long> getMissingDevices() {
    return missingDevices;
  }

  public List<Long> getExtraDevices() {
    return extraDevices;
  }
}
