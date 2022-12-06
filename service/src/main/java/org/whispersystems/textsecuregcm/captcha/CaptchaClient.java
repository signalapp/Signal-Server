/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import javax.annotation.Nullable;
import java.io.IOException;

public interface CaptchaClient {

  /**
   * @return the identifying captcha scheme that this CaptchaClient handles
   */
  String scheme();

  /**
   * Verify a provided captcha solution
   *
   * @param siteKey identifying string for the captcha service
   * @param action  an optional action indicating the purpose of the captcha
   * @param token   the captcha solution that will be verified
   * @param ip      the ip of the captcha solve
   * @return An {@link AssessmentResult} indicating whether the solution should be accepted
   * @throws IOException if the underlying captcha provider returns an error
   */
  AssessmentResult verify(
      final String siteKey,
      final @Nullable String action,
      final String token,
      final String ip) throws IOException;
}
