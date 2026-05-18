/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/// Manages post-registration change number waiting period expiration data
public class ChangeNumberWaitingPeriodManager {

  private final ChangeNumberWaitingPeriods changeNumberWaitingPeriods;
  private final Duration waitingPeriod;
  private final Clock clock;

  public ChangeNumberWaitingPeriodManager(final ChangeNumberWaitingPeriods changeNumberWaitingPeriods,
      final Duration waitingPeriod, final Clock clock) {
    this.changeNumberWaitingPeriods = changeNumberWaitingPeriods;
    this.waitingPeriod = waitingPeriod;
    this.clock = clock;
  }

  /// Must be called when an account is created, including re-registration
  @VisibleForTesting
  public CompletableFuture<Void> handleAccountCreated(final UUID aci, final Instant created) {
    return changeNumberWaitingPeriods.setExpiration(aci, created.plus(waitingPeriod));
  }

  /// Returns the waiting period duration remaining, if any. If present, {@code duration} will always be positive.
  Optional<Duration> getWaitingPeriodRemaining(final UUID aci) {
    return changeNumberWaitingPeriods.getExpiration(aci)
        .flatMap(expiration -> {
          final Duration remaining = Duration.between(clock.instant(), expiration);
          return remaining.isPositive() ? Optional.of(remaining) : Optional.empty();
        });
  }
}
