/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import java.time.Duration;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.entities.RegistrationServiceSession;

public class VerificationSessionRateLimitExceededException extends RateLimitExceededException {

  private final RegistrationServiceSession registrationServiceSession;

  /**
   * Constructs a new exception indicating when it may become safe to retry
   *
   * @param registrationServiceSession the associated registration session
   * @param retryDuration              A duration to wait before retrying, null if no duration can be indicated
   * @param legacy                     whether to use a legacy status code when mapping the exception to an HTTP
   *                                   response
   */
  public VerificationSessionRateLimitExceededException(
      final RegistrationServiceSession registrationServiceSession, @Nullable final Duration retryDuration,
      final boolean legacy) {
    super(retryDuration);
    this.registrationServiceSession = registrationServiceSession;
  }

  public RegistrationServiceSession getRegistrationSession() {
    return registrationServiceSession;
  }
}
