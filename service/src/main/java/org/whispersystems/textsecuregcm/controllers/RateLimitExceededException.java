/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import java.time.Duration;
import java.util.Optional;

public class RateLimitExceededException extends Exception {

  private final Optional<Duration> retryDuration;

  public RateLimitExceededException(final Duration retryDuration) {
    this(null, retryDuration);
  }

  public RateLimitExceededException(final String message, final Duration retryDuration) {
    super(message, null, true, false);
    // we won't provide a backoff in the case the duration is negative
    this.retryDuration = retryDuration.isNegative() ? Optional.empty() : Optional.of(retryDuration);
  }

  public Optional<Duration> getRetryDuration() { return retryDuration; }
}
