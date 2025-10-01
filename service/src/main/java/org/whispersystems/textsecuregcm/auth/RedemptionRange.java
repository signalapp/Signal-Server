/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;

/// A validated range of days for which a credential may be issued.
public class RedemptionRange implements Iterable<Instant> {

  public static final Duration MAX_REDEMPTION_DURATION = Duration.ofDays(7);

  /// The first day for which a credential should be issued
  private final LocalDate from;

  /// The last day for which a credential should be issued
  private final LocalDate end;

  private RedemptionRange(final LocalDate from, final LocalDate end) {
    this.from = from;
    this.end = end;
  }

  ///  Construct a {@link RedemptionRange} if the provided day bounds are valid.
  ///
  /// The redemption bounds must satisfy:
  ///   - `redemptionEnd` >= `redemptionStart`
  ///   - `redemptionStart` and `redemptionEnd` are day-aligned
  ///   - `redemptionStart` is yesterday or later
  ///   - `redemptionEnd` is tomorrow + `MAX_REDEMPTION_DURATION` or earlier
  ///   - The number of days requested is less than `MAX_REDEMPTION_DURATION`
  ///
  /// @param clock           Clock to use to get current day
  /// @param redemptionStart The first day included in the range
  /// @param redemptionEnd   The last day included in the range
  /// @return A {@link RedemptionRange} that can be used to iterate each day between `redemptionStart` and
  ///  `redemptionEnd`
  /// @throws IllegalArgumentException if the redemption bounds were not valid
  public static RedemptionRange inclusive(Clock clock, Instant redemptionStart, Instant redemptionEnd)
      throws IllegalArgumentException {
    final Instant today = clock.instant().truncatedTo(ChronoUnit.DAYS);
    final Instant yesterday = today.minus(Duration.ofDays(1));

    if (redemptionStart.isAfter(redemptionEnd)) {
      throw new IllegalArgumentException("end of range must be after start of range");
    }

    if (!redemptionStart.truncatedTo(ChronoUnit.DAYS).equals(redemptionStart)
        || !redemptionEnd.truncatedTo(ChronoUnit.DAYS).equals(redemptionEnd)) {
      throw new IllegalArgumentException("timestamps must be day aligned");
    }

    if (redemptionStart.isBefore(yesterday)) {
      throw new IllegalArgumentException("start of range too far in the past");
    }

    if (redemptionEnd.isAfter(today.plus(MAX_REDEMPTION_DURATION).plus(Duration.ofDays(1)))) {
      throw new IllegalArgumentException("end of range too far in the future");
    }

    if (redemptionEnd.isAfter(redemptionStart.plus(MAX_REDEMPTION_DURATION))) {
      throw new IllegalArgumentException("redemption window too large");
    }

    return new RedemptionRange(
        LocalDate.ofInstant(redemptionStart, ZoneOffset.UTC),
        LocalDate.ofInstant(redemptionEnd, ZoneOffset.UTC));
  }

  @Override
  public @NotNull Iterator<Instant> iterator() {
    final Instant fromInstant = from.atStartOfDay(ZoneOffset.UTC).toInstant();
    final Instant endInstant = end.atStartOfDay(ZoneOffset.UTC).toInstant();
    return Stream
        .iterate(fromInstant, redemptionTime -> redemptionTime.plus(Duration.ofDays(1)))
        .takeWhile(redemptionTime -> !redemptionTime.isAfter(endInstant))
        .iterator();
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RedemptionRange that = (RedemptionRange) o;
    return Objects.equals(from, that.from) && Objects.equals(end, that.end);
  }

  @Override
  public int hashCode() {
    return Objects.hash(from, end);
  }
}
