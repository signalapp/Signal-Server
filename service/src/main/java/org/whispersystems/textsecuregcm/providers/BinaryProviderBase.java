/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.providers;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.WebApplicationException;

public abstract class BinaryProviderBase {

  /**
   * Reads a UUID in network byte order and converts to a UUID object.
   */
  UUID readUuid(InputStream stream) throws IOException {
    byte[] buffer = new byte[8];

    int read = stream.readNBytes(buffer, 0, 8);
    if (read != 8) {
      throw new IOException("Insufficient bytes for UUID");
    }
    long msb = convertNetworkByteOrderToLong(buffer);

    read = stream.readNBytes(buffer, 0, 8);
    if (read != 8) {
      throw new IOException("Insufficient bytes for UUID");
    }
    long lsb = convertNetworkByteOrderToLong(buffer);

    return new UUID(msb, lsb);
  }

  private long convertNetworkByteOrderToLong(byte[] buffer) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result = (result << 8) | (buffer[i] & 0xFFL);
    }
    return result;
  }

  /**
   * Reads a varint. A varint larger than 64 bits is rejected with a {@code WebApplicationException}. An
   * {@code IOException} is thrown if the stream ends before we finish reading the varint.
   *
   * @return the varint value
   */
  static long readVarint(InputStream stream) throws IOException, WebApplicationException {
    boolean hasMore = true;
    int currentOffset = 0;
    long result = 0;
    while (hasMore) {
      if (currentOffset >= 64) {
        throw new BadRequestException("varint is too large");
      }
      int b = stream.read();
      if (b == -1) {
        throw new IOException("Missing byte " + (currentOffset / 7) + " of varint");
      }
      if (currentOffset == 63 && (b & 0xFE) != 0) {
        throw new BadRequestException("varint is too large");
      }
      hasMore = (b & 0x80) != 0;
      result |= ((long)(b & 0x7F)) << currentOffset;
      currentOffset += 7;
    }
    return result;
  }

  /**
   * Reads two bytes with most significant byte first. Treats the value as unsigned so the range returned is
   * {@code [0, 65535]}.
   */
  @VisibleForTesting
  static int readU16(InputStream stream) throws IOException {
    int b1 = stream.read();
    if (b1 == -1) {
      throw new IOException("Missing byte 1 of U16");
    }
    int b2 = stream.read();
    if (b2 == -1) {
      throw new IOException("Missing byte 2 of U16");
    }
    return (b1 << 8) | b2;
  }
}
