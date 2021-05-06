/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import java.time.Duration;

public class RetryLaterException extends Exception {
  private final Duration backoffDuration;

  public RetryLaterException(RateLimitExceededException e) {
    super(null, e, true, false);
    this.backoffDuration = e.getRetryDuration();
  }

  public Duration getBackoffDuration() { return backoffDuration; }
}
