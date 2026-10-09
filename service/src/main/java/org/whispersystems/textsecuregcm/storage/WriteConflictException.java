/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.NoStackTraceException;
import java.util.Optional;

public class WriteConflictException extends NoStackTraceException {

  @Nullable
  private final byte[] conflictedValue;

  public WriteConflictException(@Nullable final byte[] conflictedValue) {
    this.conflictedValue = conflictedValue;
  }

  /// The conflicted key, if available and relevant
  public Optional<byte[]> getConflictedValue() {
    return Optional.ofNullable(conflictedValue);
  }
}
