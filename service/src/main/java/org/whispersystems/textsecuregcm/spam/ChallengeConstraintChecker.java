/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.spam;

import jakarta.ws.rs.container.ContainerRequestContext;
import java.util.Optional;
import org.whispersystems.textsecuregcm.storage.Account;

public interface ChallengeConstraintChecker {

  record ChallengeConstraints(boolean pushPermitted, Optional<Float> captchaScoreThreshold) {}

  /**
   * Retrieve constraints for captcha and push challenges
   *
   * @param authenticatedAccount The authenticated account attempting to request or solve a challenge
   * @return ChallengeConstraints indicating what constraints should be applied to challenges
   */
  ChallengeConstraints challengeConstraints(ContainerRequestContext requestContext, Account authenticatedAccount);

  static ChallengeConstraintChecker noop() {
    return (account, ctx) -> new ChallengeConstraints(true, Optional.empty());
  }
}
