package org.whispersystems.textsecuregcm.controllers;

import java.util.List;
import java.util.Set;

public class MissingDevicesException extends Exception {
  private final List<Long> missingDevices;

  public MissingDevicesException(List<Long> missingDevices) {
    this.missingDevices = missingDevices;
  }

  public List<Long> getMissingDevices() {
    return missingDevices;
  }
}
