/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import java.util.Optional;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;

public class PendingDevicesManager {

  private final PendingDevices pendingDevices;

  public PendingDevicesManager(PendingDevices pendingDevices) {
    this.pendingDevices = pendingDevices;
  }

  public void store(String number, StoredVerificationCode code) {
    pendingDevices.insert(number, code);
  }

  public void remove(String number) {
    pendingDevices.remove(number);
  }

  public Optional<StoredVerificationCode> getCodeForNumber(String number) {
    return pendingDevices.findForNumber(number);
  }
}
