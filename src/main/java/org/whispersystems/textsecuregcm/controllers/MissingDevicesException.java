package org.whispersystems.textsecuregcm.controllers;

import java.util.List;
import java.util.Set;

public class MissingDevicesException extends Exception {
  public Set<String> missingNumbers;
  public MissingDevicesException(Set<String> missingNumbers) {
    this.missingNumbers = missingNumbers;
  }
}
