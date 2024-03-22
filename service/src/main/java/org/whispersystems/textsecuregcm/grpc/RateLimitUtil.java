/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import org.whispersystems.textsecuregcm.limits.RateLimiter;
import reactor.core.publisher.Mono;

class RateLimitUtil {

  static Mono<Void> rateLimitByRemoteAddress(final RateLimiter rateLimiter) {
    return rateLimiter.validateReactive(RequestAttributesUtil.getRemoteAddress().getHostAddress());
  }
}
