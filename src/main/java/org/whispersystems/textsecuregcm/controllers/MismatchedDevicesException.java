package org.whispersystems.textsecuregcm.controllers;

import java.util.List;
import java.util.Set;

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
