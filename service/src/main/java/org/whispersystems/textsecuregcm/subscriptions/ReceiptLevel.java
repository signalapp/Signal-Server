/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import java.util.Arrays;
import java.util.Optional;

/// This enum represents all supported purchasable items. Purchases of these items are encoded into a zero-knowledge
/// [org.signal.libsignal.zkgroup.receipts.ReceiptCredential], and the long `value` on these items map to the `level`
/// field of these credentials.
///
/// Note: the numeric `level` now mostly serves as a unique identifier for specific item-type, rather than
/// representing a comparable/ordinal value (which mostly only applies to the donation subscription levels)
public enum ReceiptLevel {
  ONE_TIME_DONATION(1L),
  ONE_TIME_GIFT_DONATION(100L),
  BACKUP_FREE(200L),
  BACKUP_PAID(201L),
  LOGIN(300L),
  SUBSCRIPTION_LOW(500),
  SUBSCRIPTION_MEDIUM(1000),
  SUBSCRIPTION_HIGH(2000);

  private static final ReceiptLevel[] VALUES = ReceiptLevel.values();

  private final long value;

  private ReceiptLevel(long value) {
    this.value = value;
  }

  public long getValue() {
    return value;
  }

  public static Optional<ReceiptLevel> lookupLevel(final long level) {
    return Arrays.stream(VALUES).filter(r -> r.value == level).findFirst();
  }
}
