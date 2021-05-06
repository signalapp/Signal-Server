/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import java.time.Duration;

public class RateLimitExceededException extends Exception {

  private final Duration retryDuration;

  public RateLimitExceededException(final Duration retryDuration) {
    this(null, retryDuration);
  }

  public RateLimitExceededException(final String message, final Duration retryDuration) {
    super(message, null, true, false);
    this.retryDuration = retryDuration;
  }

  public Duration getRetryDuration() { return retryDuration; }
}
