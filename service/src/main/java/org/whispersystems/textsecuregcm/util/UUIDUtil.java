/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UUIDUtil {

    public static byte[] toBytes(final UUID uuid) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());
        return byteBuffer.array();
    }

    public static UUID fromBytes(final byte[] bytes) {
        if (bytes.length != 16) {
            throw new IllegalArgumentException("unexpected byte array length; was " + bytes.length + " but expected 16");
        }

        final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        final long mostSigBits = byteBuffer.getLong();
        final long leastSigBits = byteBuffer.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }
}
