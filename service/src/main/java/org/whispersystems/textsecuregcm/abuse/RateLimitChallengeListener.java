/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.abuse;


import org.whispersystems.textsecuregcm.storage.Account;
import java.io.IOException;

public interface RateLimitChallengeListener {

  void handleRateLimitChallengeAnswered(Account account);

  /**
   * Configures this rate limit challenge listener. This method will be called before the service begins processing any
   * challenges.
   *
   * @param environmentName the name of the environment in which this listener is running (e.g. "staging" or "production")
   * @throws IOException if the listener could not read its configuration source for any reason
   */
  void configure(String environmentName) throws IOException;
}
