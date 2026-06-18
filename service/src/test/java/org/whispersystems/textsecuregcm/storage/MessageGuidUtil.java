/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class MessageGuidUtil {
  private static final long VERSION_MASK = 0x0000_0000_0000_f000L;
  private static final long VERSION_8_BITS = 0x0000_0000_0000_8000L;

  public static UUID generateRandomV8UUID() {
    long mostSignificantBits = ThreadLocalRandom.current().nextLong();

    // Clear any bits in the version field
    mostSignificantBits &= ~VERSION_MASK;

    // Set the version to 8
    mostSignificantBits |= VERSION_8_BITS;

    return new UUID(mostSignificantBits, ThreadLocalRandom.current().nextLong());
  }
}
