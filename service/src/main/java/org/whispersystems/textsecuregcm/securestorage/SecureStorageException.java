/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securestorage;

public class SecureStorageException extends RuntimeException {

    public SecureStorageException(final String message) {
        super(message);
    }
}
