/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/// An acknowledged message buffer keeps track of messages that have been sent, whether they have been acknowledged by
/// the recipient, and whether those acknowledgements have been flushed to the underlying data store. It also batches
/// acknowledgements into contiguous ranges in the interest of efficiency.
class AcknowledgedMessageBuffer {

  /// The maximum number of unacknowledged messages we will send before closing the stream with an error
  private final int maxUnacknowledgedMessages;

  /// Map of versionstamp -> acknowledged? to keep track of acknowledged versionstamp ranges to clear
  private final NavigableMap<Versionstamp, Boolean> sentVersionstamps = new TreeMap<>();
  private int unacknowledgedMessages = 0;

  AcknowledgedMessageBuffer(final int maxUnacknowledgedMessages) {
    this.maxUnacknowledgedMessages = maxUnacknowledgedMessages;
  }

  public synchronized void addUnacknowledgedMessage(final Versionstamp versionstamp) throws TooManyUnacknowledgedMessagesException {
    if (++unacknowledgedMessages > maxUnacknowledgedMessages) {
      throw new TooManyUnacknowledgedMessagesException();
    }

    sentVersionstamps.put(versionstamp, false);
  }

  public synchronized void acknowledgeMessage(final Versionstamp versionstamp) {
    // If we don't know about this versionstamp, or it's already acknowledged, there's nothing to do
    if (!sentVersionstamps.containsKey(versionstamp) || sentVersionstamps.get(versionstamp)) {
      return;
    }

    unacknowledgedMessages--;
    sentVersionstamps.put(versionstamp, true);
  }

  @VisibleForTesting
  int getUnacknowledgedMessageCount() {
    return unacknowledgedMessages;
  }

  /// Computes a list of acknowledged contiguous versionstamp ranges that can be cleared from the database and removes
  /// them from ths unacknowledged mesage buffer.
  ///
  /// @return a list of acknowledged contiguous versionstamp ranges that can be cleared from the database.
  public synchronized List<Pair<Versionstamp, Versionstamp>> takeFlushableRanges() {
    final List<Pair<Versionstamp, Versionstamp>> flushableRanges = computeFlushableRanges();
    flushableRanges.forEach(range -> sentVersionstamps.subMap(range.first(), true, range.second(), true).clear());

    return flushableRanges;
  }

  /// Computes a list of acknowledged contiguous versionstamp ranges that can be cleared from the database.
  ///
  /// @return a list of acknowledged contiguous versionstamp ranges that can be cleared from the database.
  @VisibleForTesting
  synchronized List<Pair<Versionstamp, Versionstamp>> computeFlushableRanges() {
    final List<Pair<Versionstamp, Versionstamp>> flushableRanges = new ArrayList<>();
    Versionstamp startInclusive = null;
    Versionstamp endInclusive = null;

    for (final Map.Entry<Versionstamp, Boolean> entry : sentVersionstamps.entrySet()) {
      if (entry.getValue()) {
        // Message is acknowledged, so we can either start tracking a new range if we aren't already, or extend our
        // current range
        if (startInclusive == null) {
          startInclusive = entry.getKey();
        }
        endInclusive = entry.getKey();
      } else {
        // Message is un-acknowledged, which means either it is a "range-breaker" or we never started tracking an
        // acknowledged range. Mark the currently tracked range flushable, if it exists.
        if (startInclusive != null) {
          assert endInclusive != null;
          flushableRanges.add(new Pair<>(startInclusive, endInclusive));
          startInclusive = null;
        }
      }
    }

    if (startInclusive != null) {
      assert endInclusive != null;
      flushableRanges.add(new Pair<>(startInclusive, endInclusive));
    }

    return flushableRanges;
  }
}
