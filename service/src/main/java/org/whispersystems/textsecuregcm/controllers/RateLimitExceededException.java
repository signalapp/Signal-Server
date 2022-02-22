/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;

public class RateLimitExceededException extends Exception {

  private final @Nullable
  Duration retryDuration;

  /**
   * Constructs a new exception indicating when it may become safe to retry
   *
   * @param retryDuration A duration to wait before retrying, null if no duration can be indicated
   */
  public RateLimitExceededException(final @Nullable Duration retryDuration) {
    super(null, null, true, false);
    this.retryDuration = retryDuration;
  }

  public Optional<Duration> getRetryDuration() {
    return Optional.ofNullable(retryDuration);
  }
}
