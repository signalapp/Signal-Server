/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import java.time.Duration;

public class RateLimitExceededException extends Exception {

  private final Duration retryDuration;

  public RateLimitExceededException() {
    super();
    retryDuration = Duration.ZERO;
  }

  public RateLimitExceededException(String message) {
    super(message);
    retryDuration = Duration.ZERO;
  }

  public RateLimitExceededException(String message, long retryAfterMillis) {
    super(message);
    retryDuration = Duration.ofMillis(retryAfterMillis);
  }

  public Duration getRetryDuration() { return retryDuration; }
}
