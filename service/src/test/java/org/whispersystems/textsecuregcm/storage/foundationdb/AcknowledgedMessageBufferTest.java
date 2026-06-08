/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage.foundationdb;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.apple.foundationdb.tuple.Versionstamp;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.util.Pair;

class AcknowledgedMessageBufferTest {

  @ParameterizedTest
  @MethodSource
  void computeFlushableRanges(final List<Integer> versionstampsSent,
      final List<Integer> versionstampsAcknowledged,
      final List<Pair<Integer, Integer>> expectedRanges) {

    final AcknowledgedMessageBuffer acknowledgedMessageBuffer =
        new AcknowledgedMessageBuffer(FoundationDbMessageStream.DEFAULT_MAX_UNACKNOWLEDGED_MESSAGES);

    versionstampsSent.forEach(versionstamp -> assertDoesNotThrow(() ->
        acknowledgedMessageBuffer.addUnacknowledgedMessage(versionstampFromInt(versionstamp))));

    versionstampsAcknowledged.forEach(versionstamp ->
        acknowledgedMessageBuffer.acknowledgeMessage(versionstampFromInt(versionstamp)));

    final List<Pair<Versionstamp, Versionstamp>> flushableRanges = acknowledgedMessageBuffer.computeFlushableRanges();

    assertEquals(expectedRanges
            .stream()
            .map(range -> new Pair<>(versionstampFromInt(range.first()), versionstampFromInt(range.second())))
            .toList(),
        flushableRanges);
  }

  @Test
  void takeFlushableRanges() {
    final AcknowledgedMessageBuffer acknowledgedMessageBuffer =
        new AcknowledgedMessageBuffer(FoundationDbMessageStream.DEFAULT_MAX_UNACKNOWLEDGED_MESSAGES);

    IntStream.range(0, 10)
        .mapToObj(AcknowledgedMessageBufferTest::versionstampFromInt)
        .forEach(versionstamp -> assertDoesNotThrow(
            () -> acknowledgedMessageBuffer.addUnacknowledgedMessage(versionstamp)));

    assertEquals(10, acknowledgedMessageBuffer.getUnacknowledgedMessageCount());

    IntStream.range(2, 8)
        .mapToObj(AcknowledgedMessageBufferTest::versionstampFromInt)
        .forEach(acknowledgedMessageBuffer::acknowledgeMessage);

    assertEquals(List.of(new Pair<>(versionstampFromInt(2), versionstampFromInt(7))),
        acknowledgedMessageBuffer.takeFlushableRanges());

    assertEquals(4, acknowledgedMessageBuffer.getUnacknowledgedMessageCount());
  }

  static Stream<Arguments> computeFlushableRanges() {
    return Stream.of(
        Arguments.argumentSet("Single acknowledged message range", List.of(1, 2, 3), List.of(1),
            List.of(new Pair<>(1, 1))),
        Arguments.argumentSet("No acknowledged messages", List.of(1, 2, 3), Collections.emptyList(),
            Collections.emptyList()),
        Arguments.argumentSet("Fully acknowledged message range", List.of(1, 2, 3, 4),
            List.of(1, 2, 3, 4), List.of(new Pair<>(1, 4))),
        Arguments.argumentSet("Not all messages acknowledged", List.of(1, 2, 3, 4),
            List.of(1, 2, 3), List.of(new Pair<>(1, 3))),
        Arguments.argumentSet("Out-of-order acknowledged ranges", List.of(1, 2, 3, 4, 5, 6, 7, 8),
            List.of(1, 3, 4, 5, 7, 8), List.of(new Pair<>(1, 1), new Pair<>(3, 5), new Pair<>(7, 8))),
        Arguments.argumentSet("Out-of-order acknowledged single messages", List.of(1, 2, 3, 4, 5, 6, 7, 8),
            List.of(1, 3, 5), List.of(new Pair<>(1, 1), new Pair<>(3, 3), new Pair<>(5, 5)))
    );
  }

  private static Versionstamp versionstampFromInt(final int version) {
    final ByteBuffer buf = ByteBuffer.allocate(10).order(ByteOrder.BIG_ENDIAN);
    buf.putLong(version);      // 8 bytes: transaction version
    buf.putShort((short) 2);   // 2 bytes: batch order within transaction
    return Versionstamp.complete(buf.array());
  }
}
