/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;

class RateLimitUtil {

  static void rateLimitByRemoteAddress(final RateLimiter rateLimiter) throws RateLimitExceededException {
    rateLimiter.validate(RequestAttributesUtil.getRemoteAddress().getHostAddress());
  }
}
