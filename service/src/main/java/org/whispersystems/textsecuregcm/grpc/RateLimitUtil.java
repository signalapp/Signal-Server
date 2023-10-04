/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import reactor.core.publisher.Mono;
import java.net.SocketAddress;
import java.time.Duration;

class RateLimitUtil {

  private static final RateLimitExceededException UNKNOWN_REMOTE_ADDRESS_EXCEPTION =
      new RateLimitExceededException(Duration.ofHours(1), true);

  static Mono<Void> rateLimitByRemoteAddress(final RateLimiter rateLimiter) {
    return rateLimitByRemoteAddress(rateLimiter, true);
  }

  static Mono<Void> rateLimitByRemoteAddress(final RateLimiter rateLimiter, final boolean failOnUnknownRemoteAddress) {
    final SocketAddress remoteAddress = RemoteAddressUtil.getRemoteAddress();

    if (remoteAddress != null) {
      return rateLimiter.validateReactive(remoteAddress.toString());
    } else {
      return failOnUnknownRemoteAddress ? Mono.error(UNKNOWN_REMOTE_ADDRESS_EXCEPTION) : Mono.empty();
    }
  }
}
