/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UUIDUtil {

    public static byte[] toBytes(final UUID uuid) {
        return toByteBuffer(uuid).array();
    }

    public static ByteBuffer toByteBuffer(final UUID uuid) {
      final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
      byteBuffer.putLong(uuid.getMostSignificantBits());
      byteBuffer.putLong(uuid.getLeastSignificantBits());
      return byteBuffer.flip();
    }

    public static UUID fromBytes(final byte[] bytes) {
      return fromByteBuffer(ByteBuffer.wrap(bytes));
    }

    public static UUID fromByteBuffer(final ByteBuffer byteBuffer) {
      if (byteBuffer.array().length != 16) {
        throw new IllegalArgumentException("unexpected byte array length; was " + byteBuffer.array().length + " but expected 16");
      }

      final long mostSigBits = byteBuffer.getLong();
      final long leastSigBits = byteBuffer.getLong();
      return new UUID(mostSigBits, leastSigBits);
  }
}
