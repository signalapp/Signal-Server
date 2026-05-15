/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.SetArgs;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;

/// Manages post-registration change number waiting period expiration data
public class ChangeNumberWaitingPeriodManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeNumberWaitingPeriodManager.class);

  private final FaultTolerantRedisClusterClient redisCluster;
  private final Duration waitingPeriod;

  public ChangeNumberWaitingPeriodManager(final FaultTolerantRedisClusterClient redisCluster, final Duration waitingPeriod) {
    this.redisCluster = redisCluster;
    this.waitingPeriod = waitingPeriod;
  }

  /// Must be called when an account is created, including re-registration
  @VisibleForTesting
  public CompletableFuture<Void> handleAccountCreated(final UUID aci, final Instant created) {
    return redisCluster.withCluster(conn -> conn.async().set(key(aci), "", SetArgs.Builder.exAt(created.plus(waitingPeriod))))
        .toCompletableFuture()
        .thenApply(_ -> null);
  }

  /// Returns the waiting period duration remaining, if any. If present, {@code duration} will always be positive.
  Optional<Duration> getWaitingPeriodRemaining(final UUID aci) {
    final long ttlMillis = redisCluster.withCluster(conn -> conn.sync().ttl(key(aci)));

    if (ttlMillis == -1) {
      // key present without TTL. This should never happen.
      LOGGER.error("No expiration for {}", aci);
      throw new RuntimeException("No expiration for key that must always have a expiration");
    }

    if (ttlMillis == -2) {
      // key did not exist
      return Optional.empty();
    }

    final Duration remaining = Duration.ofMillis(ttlMillis);

    return remaining.isPositive() ? Optional.of(remaining) : Optional.empty();
  }

  @VisibleForTesting
  static String key(final UUID aci) {
    return "changeNumberWaiting::{" + aci + "}";
  }
}
