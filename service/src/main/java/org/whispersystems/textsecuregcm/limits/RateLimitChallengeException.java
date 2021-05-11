package org.whispersystems.textsecuregcm.limits;

import org.whispersystems.textsecuregcm.storage.Account;
import java.time.Duration;

public class RateLimitChallengeException extends Exception {

  private final Account account;
  private final Duration retryAfter;

  public RateLimitChallengeException(final Account account, final Duration retryAfter) {
    this.account = account;
    this.retryAfter = retryAfter;
  }

  public Account getAccount() {
    return account;
  }

  public Duration getRetryAfter() {
    return retryAfter;
  }
}
