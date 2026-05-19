/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.Optional;
import org.whispersystems.textsecuregcm.registration.VerificationSession;

public class VerificationSessionManager {

  private final VerificationSessions verificationSessions;

  public VerificationSessionManager(final VerificationSessions verificationSessions) {
    this.verificationSessions = verificationSessions;
  }

  public void insert(final VerificationSession verificationSession) {
    verificationSessions.insert(verificationSession.sessionId(), verificationSession);
  }

  public void update(final VerificationSession verificationSession) {
    verificationSessions.update(verificationSession.sessionId(), verificationSession);
  }

  public Optional<VerificationSession> findForId(final String encodedSessionId) {
    return verificationSessions.findForKey(encodedSessionId);
  }

}
