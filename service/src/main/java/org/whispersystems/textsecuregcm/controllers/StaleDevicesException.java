/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import java.util.List;


public class StaleDevicesException extends Exception {

  private final List<Byte> staleDevices;

  public StaleDevicesException(List<Byte> staleDevices) {
    this.staleDevices = staleDevices;
  }

  public List<Byte> getStaleDevices() {
    return staleDevices;
  }
}
