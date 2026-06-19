/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage.foundationdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.storage.FoundationDbClusterExtension;
import org.whispersystems.textsecuregcm.util.TestClock;

import com.apple.foundationdb.tuple.Versionstamp;

class VersionstampClockTest {
  @RegisterExtension
  static FoundationDbClusterExtension FOUNDATION_DB_EXTENSION = new FoundationDbClusterExtension(1);

  private final TestClock clock = TestClock.now();
  private final VersionstampClock versionstampClock = new VersionstampClock(
      FOUNDATION_DB_EXTENSION.getDatabases()[0], clock);

  @BeforeEach
  void setup() {
    FOUNDATION_DB_EXTENSION.getDatabases()[0].run(transaction -> {
      transaction.clear(VersionstampClock.SUBSPACE.range());
      return null;
    });
  }

  @Test
  void noEntries() {
    assertThat(versionstampClock.getVersionstamp(clock.instant())).isEmpty();
  }

  @Test
  void recordVersionstamp() {
    final Instant start = clock.instant();
    final Instant firstInsert = start.plus(Duration.ofSeconds(1));
    final Instant betweenInserts = start.plus(Duration.ofMinutes(30));
    final Instant secondInsert = start.plus(Duration.ofHours(1));
    final Instant tomorrow = start.plus(Duration.ofDays(1));

    clock.pin(firstInsert);
    final Versionstamp firstStamp = versionstampClock.recordVersionstampAndTime().join();

    clock.pin(secondInsert);
    final Versionstamp secondStamp = versionstampClock.recordVersionstampAndTime().join();

    assertThat(versionstampClock.getVersionstamp(start)).isEmpty();
    assertThat(versionstampClock.getVersionstamp(firstInsert)).isPresent().hasValue(firstStamp);
    assertThat(versionstampClock.getVersionstamp(betweenInserts)).isPresent().hasValue(firstStamp);
    assertThat(versionstampClock.getVersionstamp(secondInsert)).isPresent().hasValue(secondStamp);
    assertThat(versionstampClock.getVersionstamp(tomorrow)).isPresent().hasValue(secondStamp);
  }

  @Test
  void clearExpiredEntries() {
    final Instant start = clock.instant();
    final Instant veryOld = start.minus(Duration.ofDays(45));
    final Instant old = start.minus(Duration.ofDays(21));

    clock.pin(veryOld);
    final Versionstamp veryOldStamp = versionstampClock.recordVersionstampAndTime().join();

    clock.pin(old);
    final Versionstamp oldStamp = versionstampClock.recordVersionstampAndTime().join();

    clock.pin(start);
    final Versionstamp nowStamp = versionstampClock.recordVersionstampAndTime().join();

    assertThat(versionstampClock.getVersionstamp(veryOld)).isPresent().hasValue(veryOldStamp);

    versionstampClock.clearExpiredEntries(old);

    assertThat(versionstampClock.getVersionstamp(veryOld)).isEmpty();
    assertThat(versionstampClock.getVersionstamp(old)).isPresent().hasValue(oldStamp);
    assertThat(versionstampClock.getVersionstamp(start)).isPresent().hasValue(nowStamp);
  }
}
