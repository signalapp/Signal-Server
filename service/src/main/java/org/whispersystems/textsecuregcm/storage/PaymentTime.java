/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * The time at which a receipt was purchased. Some providers provide the end of the period, others the beginning. Either
 * way, lets you calculate the expiration time for a product associated with the payment.
 * <p>
 * A subscription is typically for a fixed pay period. For example, a subscription may require renewal every 30 days.
 * Until the end of a period, a subscriber may create a receipt credential that can be cashed in for access to the
 * purchase. This receipt credential has an expiration that at least includes the end of the payment period but may
 * additionally include allowance (gracePeriod) for missed payments. The product obtained with the receipt will be
 * usable until this expiration time.
 */
public class PaymentTime {

  @Nullable
  Instant periodStart;
  @Nullable
  Instant periodEnd;

  private PaymentTime(@Nullable Instant periodStart, @Nullable Instant periodEnd) {
    if ((periodStart == null && periodEnd == null) || (periodStart != null && periodEnd != null)) {
      throw new IllegalArgumentException("Only one of periodStart and periodEnd should be provided");
    }
    this.periodStart = periodStart;
    this.periodEnd = periodEnd;
  }

  public static PaymentTime periodEnds(Instant periodEnd) {
    return new PaymentTime(null, Objects.requireNonNull(periodEnd));
  }

  public static PaymentTime periodStart(Instant periodStart) {
    return new PaymentTime(Objects.requireNonNull(periodStart), null);
  }

  /**
   * Calculate the expiration time for this period
   *
   * @param periodLength How long after the time of payment should the receipt be valid
   * @param gracePeriod  An additional grace period after the end of the period to add to the expiration
   * @return Instant when the receipt should expire
   */
  public Instant receiptExpiration(final Duration periodLength, final Duration gracePeriod) {
    final Instant expiration = periodStart != null
        ? periodStart.plus(periodLength).plus(gracePeriod)
        : periodEnd.plus(gracePeriod);

    return expiration.truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.DAYS);
  }
}
