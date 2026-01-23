/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.backup;

public class BackupBadReceiptException extends BackupException {

  public BackupBadReceiptException(String message) {
    super(message);
  }
}
