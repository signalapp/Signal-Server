/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import java.util.ArrayList;
import java.util.List;
import org.whispersystems.textsecuregcm.storage.Account;

public class RateLimitChallengeOptionManager {

  private final RateLimiters rateLimiters;

  public static final String OPTION_CAPTCHA = "captcha";
  public static final String OPTION_PUSH_CHALLENGE = "pushChallenge";

  public RateLimitChallengeOptionManager(final RateLimiters rateLimiters) {
    this.rateLimiters = rateLimiters;
  }

  public List<String> getChallengeOptions(final Account account) {
    final List<String> options = new ArrayList<>(2);

    if (rateLimiters.getCaptchaChallengeAttemptLimiter().hasAvailablePermits(account.getUuid(), 1) &&
        rateLimiters.getCaptchaChallengeSuccessLimiter().hasAvailablePermits(account.getUuid(), 1)) {

      options.add(OPTION_CAPTCHA);
    }

    if (rateLimiters.getPushChallengeAttemptLimiter().hasAvailablePermits(account.getUuid(), 1) &&
        rateLimiters.getPushChallengeSuccessLimiter().hasAvailablePermits(account.getUuid(), 1)) {

      options.add(OPTION_PUSH_CHALLENGE);
    }

    return options;
  }
}
