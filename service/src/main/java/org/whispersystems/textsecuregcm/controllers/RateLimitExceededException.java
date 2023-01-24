/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nullable;

public class RateLimitExceededException extends Exception {

  @Nullable
  private final Duration retryDuration;
  private final boolean legacy;

  /**
   * Constructs a new exception indicating when it may become safe to retry
   *
   * @param retryDuration A duration to wait before retrying, null if no duration can be indicated
   * @param legacy        whether to use a legacy status code when mapping the exception to an HTTP response
   */
  public RateLimitExceededException(@Nullable final Duration retryDuration, final boolean legacy) {
    super(null, null, true, false);
    this.retryDuration = retryDuration;
    this.legacy = legacy;
  }

  public Optional<Duration> getRetryDuration() {
    return Optional.ofNullable(retryDuration);
  }

  public boolean isLegacy() {
    return legacy;
  }
}
