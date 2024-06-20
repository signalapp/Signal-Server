/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.backup;

import org.whispersystems.textsecuregcm.util.ExceptionUtils;

import javax.annotation.Nullable;
import java.util.Optional;


/**
 * The result of a copy operation
 *
 * @param outcome Whether the copy was a success
 * @param mediaId The destination mediaId
 * @param cdn On success, the destination cdn
 */
public record CopyResult(Outcome outcome, byte[] mediaId, @Nullable Integer cdn) {

  public enum Outcome {
    SUCCESS,
    SOURCE_NOT_FOUND,
    SOURCE_WRONG_LENGTH,
    OUT_OF_QUOTA
  }

  /**
   * Map an exception returned by {@link RemoteStorageManager#copy} to CopyResult with the appropriate outcome.
   *
   * @param throwable result of a failed copy operation
   * @param key the copy destination mediaId
   * @return The appropriate CopyResult, or empty if the exception does not match to an Outcome.
   */
  static Optional<CopyResult> fromCopyError(final Throwable throwable, final byte[] key) {
    final Throwable unwrapped = ExceptionUtils.unwrap(throwable);
    if (unwrapped instanceof SourceObjectNotFoundException) {
      return Optional.of(new CopyResult(Outcome.SOURCE_NOT_FOUND, key, null));
    } else if (unwrapped instanceof InvalidLengthException) {
      return Optional.of(new CopyResult(Outcome.SOURCE_WRONG_LENGTH, key, null));
    } else {
      return Optional.empty();
    }
  }
}
