package org.whispersystems.textsecuregcm.spam;

import jakarta.ws.rs.container.ContainerRequestContext;

public interface RegistrationRecoveryChecker {

  /**
   * Determine if a registration recovery attempt should be allowed or not
   *
   * @param requestContext The container request context for a registration recovery attempt
   * @param e164           The E164 formatted phone number of the requester
   * @return true if the registration recovery attempt is allowed, false otherwise.
   */
  boolean checkRegistrationRecoveryAttempt(final ContainerRequestContext requestContext, final String e164);

  static RegistrationRecoveryChecker noop() {
    return (ignoredCtx, ignoredE164) ->  true;
  }
}
