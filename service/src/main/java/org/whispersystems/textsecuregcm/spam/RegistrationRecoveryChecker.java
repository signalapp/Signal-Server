package org.whispersystems.textsecuregcm.spam;

import jakarta.annotation.Nullable;

public interface RegistrationRecoveryChecker {

  /**
   * Determine if a registration recovery attempt should be allowed or not
   *
   * @param requestContext The container request context for a registration recovery attempt
   * @param e164           The E164 formatted phone number of the requester
   * @return true if the registration recovery attempt is allowed, false otherwise.
   */
  boolean checkRegistrationRecoveryAttempt(String e164, @Nullable String userAgent, @Nullable String acceptLanguage, @Nullable String mostRecentProxy);

  static RegistrationRecoveryChecker noop() {
    return (_, _, _, _) -> true;
  }
}
