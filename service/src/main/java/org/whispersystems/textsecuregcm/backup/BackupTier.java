/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Maps receipt levels to BackupTiers. Existing receipt levels should never be remapped to a different tier.
 * <p>
 * Today, receipt levels 1:1 correspond to tiers, but in the future multiple receipt levels may be accepted for access
 * to a single tier.
 */
public enum BackupTier {
  NONE(0),
  MESSAGES(200),
  MEDIA(201);

  private static Map<Long, BackupTier> LOOKUP = Arrays.stream(BackupTier.values())
      .collect(Collectors.toMap(BackupTier::getReceiptLevel, Function.identity()));
  private long receiptLevel;

  BackupTier(long receiptLevel) {
    this.receiptLevel = receiptLevel;
  }

  long getReceiptLevel() {
    return receiptLevel;
  }

  public static Optional<BackupTier> fromReceiptLevel(long receiptLevel) {
    return Optional.ofNullable(LOOKUP.get(receiptLevel));
  }
}
