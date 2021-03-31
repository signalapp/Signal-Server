/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securebackup;

public class SecureBackupException extends RuntimeException {

    public SecureBackupException(final String message) {
        super(message);
    }
}
