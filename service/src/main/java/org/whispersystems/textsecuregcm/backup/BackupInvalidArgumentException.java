/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.backup;

public class BackupInvalidArgumentException extends BackupException {
  public BackupInvalidArgumentException(final String message) {
    super(message);
  }
}
