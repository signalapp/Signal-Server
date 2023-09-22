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

public enum BackupTier {
  NONE(0),
  MESSAGES(10),
  MEDIA(20);

  private static Map<Long, BackupTier> LOOKUP = Arrays.stream(BackupTier.values())
      .collect(Collectors.toMap(BackupTier::getReceiptLevel, Function.identity()));
  private long receiptLevel;

  private BackupTier(long receiptLevel) {
    this.receiptLevel = receiptLevel;
  }

  long getReceiptLevel() {
    return receiptLevel;
  }

  static Optional<BackupTier> fromReceiptLevel(long receiptLevel) {
    return Optional.ofNullable(LOOKUP.get(receiptLevel));
  }
}
