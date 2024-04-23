/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.backup;

import org.signal.libsignal.zkgroup.backups.BackupLevel;

public class BackupLevelUtil {
  public static BackupLevel fromReceiptLevel(long receiptLevel) {
    try {
      return BackupLevel.fromValue(Math.toIntExact(receiptLevel));
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("Invalid receipt level: " + receiptLevel);
    }
  }
}
