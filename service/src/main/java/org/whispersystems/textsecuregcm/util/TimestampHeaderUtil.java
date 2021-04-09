/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

public class TimestampHeaderUtil {

    public static final String TIMESTAMP_HEADER = "X-Signal-Timestamp";

    private TimestampHeaderUtil() {
    }

    public static String getTimestampHeader() {
        return TIMESTAMP_HEADER + ":" + System.currentTimeMillis();
    }
}
