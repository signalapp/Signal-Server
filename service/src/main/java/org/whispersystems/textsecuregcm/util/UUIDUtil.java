/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import java.nio.BufferUnderflowException;
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
      try {
        final long mostSigBits = byteBuffer.getLong();
        final long leastSigBits = byteBuffer.getLong();
        if (byteBuffer.hasRemaining()) {
          throw new IllegalArgumentException("unexpected byte array length; was greater than 16");
        }
        return new UUID(mostSigBits, leastSigBits);
      } catch (BufferUnderflowException e) {
        throw new IllegalArgumentException("unexpected byte array length; was less than 16");
      }
  }
}
