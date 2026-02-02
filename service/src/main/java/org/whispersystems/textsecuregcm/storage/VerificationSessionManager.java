/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.whispersystems.textsecuregcm.registration.VerificationSession;

public class VerificationSessionManager {

  private final VerificationSessions verificationSessions;

  public VerificationSessionManager(final VerificationSessions verificationSessions) {
    this.verificationSessions = verificationSessions;
  }

  public CompletableFuture<Void> insert(final VerificationSession verificationSession) {
    return verificationSessions.insert(verificationSession.sessionId(), verificationSession);
  }

  public CompletableFuture<Void> update(final VerificationSession verificationSession) {
    return verificationSessions.update(verificationSession.sessionId(), verificationSession);
  }

  public CompletableFuture<Optional<VerificationSession>> findForId(final String encodedSessionId) {
    return verificationSessions.findForKey(encodedSessionId);
  }

}
