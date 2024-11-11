/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.spam;

import jakarta.ws.rs.container.ContainerRequestContext;
import java.util.Optional;
import org.whispersystems.textsecuregcm.entities.UpdateVerificationSessionRequest;
import org.whispersystems.textsecuregcm.registration.VerificationSession;

public interface RegistrationFraudChecker {

  record VerificationCheck(Optional<VerificationSession> updatedSession, Optional<Float> scoreThreshold) {}

  /**
   * Determine if a registration attempt is suspicious
   *
   * @param requestContext      The request context for an update verification session attempt
   * @param verificationSession The target verification session
   * @param e164                The target phone number
   * @param request             The information to add to the verification session
   * @return A SessionUpdate including updates to the verification session that should be persisted to the caller along
   * with other constraints to enforce when evaluating the UpdateVerificationSessionRequest.
   */
  VerificationCheck checkVerificationAttempt(
      final ContainerRequestContext requestContext,
      final VerificationSession verificationSession,
      final String e164,
      final UpdateVerificationSessionRequest request);

  static RegistrationFraudChecker noop() {
    return (ignoredContext, ignoredSession, ignoredE164, ignoredRequest) -> new VerificationCheck(
        Optional.empty(),
        Optional.empty());
  }
}
