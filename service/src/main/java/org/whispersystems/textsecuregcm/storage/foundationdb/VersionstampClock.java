/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Instant;
import java.util.Iterator;
import java.util.Optional;

 /// A "versionstamp clock" records versionstamp/time pairs in a FoundationDB database. The purpose of the "clock" is to
 /// allow callers to get the versionstamp of a FoundationDB database at a specific real-world time to facilitate
 /// time-related range queries (e.g. "clear all keys with a versionstamp less than or equal to the versionstamp from
 /// seven days ago, since those keys are now expired").
 ///
 /// Some external component should call [#recordVersionstampAndTime()] at regular, application-appropriate
 /// intervals to maintain a running mapping of real-world timestamps to FoundationDB versionstamps.
public class VersionstampClock {

  @VisibleForTesting
  static final Subspace SUBSPACE = new Subspace(Tuple.from("V"));

  private final Database database;
  private final Clock clock;

  public VersionstampClock(final Database database) {
    this(database, Clock.systemUTC());
  }

  @VisibleForTesting
  public VersionstampClock(final Database database, final Clock clock) {
    this.database = database;
    this.clock = clock;
  }

  /// Make a recording in the database of the current time and associated versionstamp.
  ///
  /// @return the versionstamp for the newly-recorded entry
  public Versionstamp recordVersionstampAndTime() {
    final Instant currentTime = clock.instant();

    return database.run(transaction -> {
          transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE,
              getTimestampKey(currentTime),
              Tuple.from(Versionstamp.incomplete()).packWithVersionstamp());

          return transaction.getVersionstamp();
        })
        .thenApply(Versionstamp::complete)
        .join();
  }

  /// Returns the highest versionstamp recorded at or before the given instant.
  ///
  /// @param timestamp the time for which to retrieve a versionstamp
  ///
  /// @return the highest versionstamp recorded at or before the given instant, or empty if no versionstamps have been recorded before the given instant
  public Optional<Versionstamp> getVersionstamp(final Instant timestamp) {
    return database.read(transaction -> {

      final byte[] rangeStart = SUBSPACE.getKey();
      final byte[] rangeEnd = getTimestampKey(timestamp.plusMillis(1));

      final Iterator<KeyValue> keyValueIterator = transaction.getRange(rangeStart, rangeEnd, 1, true).iterator();

      if (keyValueIterator.hasNext()) {
        return Optional.of(Tuple.fromBytes(keyValueIterator.next().getValue()).getVersionstamp(0));
      }

      return Optional.empty();
    });
  }

  /// Remove any entries from the versionstamp-clock namespace that are strictly older than the given timestamp
  ///
  /// @param oldestRetainedEntryTimestamp the earliest time for which we still want to be able to obtain a versionstamp
  public void clearExpiredEntries(final Instant oldestRetainedEntryTimestamp) {
    database.run(transaction -> {
      transaction.clear(SUBSPACE.getKey(), getTimestampKey(oldestRetainedEntryTimestamp));
      return null;
    });
  }

  private byte[] getTimestampKey(final Instant timestamp) {
    return SUBSPACE.pack(timestamp.toEpochMilli());
  }
}
