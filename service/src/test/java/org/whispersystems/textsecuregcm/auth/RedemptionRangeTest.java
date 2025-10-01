/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.util.TestClock;


class RedemptionRangeTest {

  static List<Arguments> invalidCredentialTimeWindows() {
    final Duration max = RedemptionRange.MAX_REDEMPTION_DURATION;
    final Instant day0 = Instant.EPOCH;
    final Instant day1 = Instant.EPOCH.plus(Duration.ofDays(1));
    final Instant day2 = Instant.EPOCH.plus(Duration.ofDays(2));
    return List.of(
        Arguments.argumentSet("non-truncated start", Instant.ofEpochSecond(100), day0.plus(max),
            Instant.ofEpochSecond(100)),
        Arguments.argumentSet("non-truncated end", day0, Instant.ofEpochSecond(1).plus(max),
            Instant.ofEpochSecond(100)),
        Arguments.argumentSet("start too old", day0, day0.plus(max), day2),
        Arguments.argumentSet("end too far in the future", day2, day2.plus(max), day0),
        Arguments.argumentSet("end before start", day1, day0, day1),
        Arguments.argumentSet("window too big", day0, day0.plus(max).plus(Duration.ofDays(1)),  day1)
    );
  }

  @ParameterizedTest
  @MethodSource
  void invalidCredentialTimeWindows(final Instant requestRedemptionStart, final Instant requestRedemptionEnd,
      final Instant now) {
    final Clock clock = TestClock.pinned(now);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> RedemptionRange.inclusive(clock, requestRedemptionStart, requestRedemptionEnd));
  }

  @Test
  void allowUpToMax() {
    final Instant now = Instant.EPOCH.plus(Duration.ofDays(1)).plus(Duration.ofSeconds(100));
    final Instant today = now.truncatedTo(ChronoUnit.DAYS);
    final Clock clock = TestClock.pinned(now);
    for (Duration d = Duration.ofDays(0);
        d.compareTo(RedemptionRange.MAX_REDEMPTION_DURATION) <= 0;
        d = d.plus(Duration.ofDays(1))) {
      final Duration fd = d;
      assertThatNoException().isThrownBy(() -> RedemptionRange.inclusive(clock, today, today.plus(fd)));
    }
  }

  @Test
  void allowBackwardsSkew() {
    final Instant now = Instant.EPOCH.plus(Duration.ofDays(1)).plus(Duration.ofSeconds(100));
    final Instant yesterday = now.minus(Duration.ofDays(1)).truncatedTo(ChronoUnit.DAYS);
    final Clock clock = TestClock.pinned(now);
    assertThatNoException().isThrownBy(() ->
        RedemptionRange.inclusive(clock, yesterday, yesterday.plus(RedemptionRange.MAX_REDEMPTION_DURATION)));
  }

  @Test
  void allowForwardsSkew() {
    final Instant now = Instant.EPOCH.plus(Duration.ofDays(1)).plus(Duration.ofSeconds(100));
    final Instant tomorrow = now.plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.DAYS);
    final Clock clock = TestClock.pinned(now);
    assertThatNoException().isThrownBy(() ->
        RedemptionRange.inclusive(clock, tomorrow, tomorrow.plus(RedemptionRange.MAX_REDEMPTION_DURATION)));
  }

  @Test
  void inclusiveRange() {
    final Instant now = Instant.EPOCH.plus(Duration.ofDays(1)).plus(Duration.ofSeconds(100));
    final Instant today = now.truncatedTo(ChronoUnit.DAYS);
    final Clock clock = TestClock.pinned(now);
    for (int numDays = 0; numDays < 7; numDays++) {
      final RedemptionRange range = RedemptionRange.inclusive(clock, today, today.plus(Duration.ofDays(numDays)));
      final List<Instant> instants = StreamSupport.stream(range.spliterator(), false).toList();
      assertThat(instants.size()).isEqualTo(numDays + 1);
    }
  }
}
