/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.ua;

public class UnrecognizedUserAgentException extends Exception {

    public UnrecognizedUserAgentException() {
    }

    public UnrecognizedUserAgentException(final String message) {
        super(message);
    }

    public UnrecognizedUserAgentException(final Throwable cause) {
        super(cause);
    }
}
