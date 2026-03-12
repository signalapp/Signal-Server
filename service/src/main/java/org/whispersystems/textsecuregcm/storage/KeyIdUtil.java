/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

public class KeyIdUtil {
  public static final long MAX_KEY_ID = (1L << 32) - 1;
  public static final long MIN_KEY_ID = 0;
  private KeyIdUtil(){}

  public static boolean keyIdValid(final long keyId) {
    return keyId <= MAX_KEY_ID && keyId >= MIN_KEY_ID;
  }

  /// Convert a long keyId (a 32-bit unsigned int) into an int representation.
  ///
  /// The inverse of [Integer#toUnsignedLong].
  ///
  /// @param keyId A key ID which must be in the range [0, 2^32)
  /// @throws IllegalArgumentException If `keyId` is not within the range
  /// @return A 32-bit unsigned integer where the top bit is stored in the sign bit
  public static int toUnsignedInt(final long keyId) {
    if (!keyIdValid(keyId)) {
      throw new IllegalArgumentException("Invalid keyId " + keyId);
    }
    return (int) keyId;
  }
}
