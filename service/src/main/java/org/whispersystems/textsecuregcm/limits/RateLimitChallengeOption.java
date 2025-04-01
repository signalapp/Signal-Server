/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

public enum RateLimitChallengeOption {
  CAPTCHA("captcha"),
  PUSH_CHALLENGE("pushChallenge");

  private final String apiName;

  RateLimitChallengeOption(final String apiName) {
    this.apiName = apiName;
  }

  public String getApiName() {
    return apiName;
  }
}
