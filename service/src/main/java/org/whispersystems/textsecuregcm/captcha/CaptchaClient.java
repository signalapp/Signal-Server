/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface CaptchaClient {


  /**
   * @return the identifying captcha scheme that this CaptchaClient handles
   */
  String scheme();

  /**
   * @param action the action to retrieve site keys for
   * @return siteKeys this client is willing to accept
   */
  Set<String> validSiteKeys(final Action action);

  /**
   * Verify a provided captcha solution
   *
   * @param maybeAci  optional account service identifier of the user
   * @param siteKey   identifying string for the captcha service
   * @param action    an action indicating the purpose of the captcha
   * @param token     the captcha solution that will be verified
   * @param ip        the ip of the captcha solver
   * @param userAgent the User-Agent string of the captcha solver
   * @return An {@link AssessmentResult} indicating whether the solution should be accepted
   * @throws IOException if the underlying captcha provider returns an error
   */
  AssessmentResult verify(
      final Optional<UUID> maybeAci,
      final String siteKey,
      final Action action,
      final String token,
      final String ip,
      final String userAgent) throws IOException;

  static CaptchaClient noop() {
    return new CaptchaClient() {
      @Override
      public String scheme() {
        return "noop";
      }

      @Override
      public Set<String> validSiteKeys(final Action action) {
        return Set.of("noop");
      }

      @Override
      public AssessmentResult verify(final Optional<UUID> maybeAci, final String siteKey, final Action action, final String token, final String ip,
          final String userAgent) throws IOException {
        return AssessmentResult.alwaysValid();
      }
    };
  }
}
