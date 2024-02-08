/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.spam;


import org.whispersystems.textsecuregcm.storage.Account;
import java.io.IOException;

public interface RateLimitChallengeListener {

  void handleRateLimitChallengeAnswered(Account account, ChallengeType type);
}
