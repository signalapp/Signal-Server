/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import java.time.Duration;

public class RetryLaterException extends Exception {
  private final Duration backoffDuration;

  public RetryLaterException() {
    backoffDuration = Duration.ZERO;
  }
  public RetryLaterException(int retryLaterMillis) {
    backoffDuration = Duration.ofMillis(retryLaterMillis);
  }

  public RetryLaterException(RateLimitExceededException e) {
    this.backoffDuration = e.getRetryDuration();
  }

  public Duration getBackoffDuration() { return backoffDuration; }
}
