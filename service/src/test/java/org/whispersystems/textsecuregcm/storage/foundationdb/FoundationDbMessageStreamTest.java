package org.whispersystems.textsecuregcm.storage.foundationdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Versionstamp;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;

class FoundationDbMessageStreamTest {

  @ParameterizedTest
  @MethodSource
  void computeFlushableRanges(final List<Integer> versionstampsSent, final List<Integer> versionstampsAcknowledged,
      final List<Pair<Integer, Integer>> expectedRanges) {
    final FoundationDbMessageStream foundationDbMessageStream = new FoundationDbMessageStream(
        mock(Subspace.class),
        new byte[]{0},
        mock(Database.class),
        mock(MessageGuidCodec.class),
        100,
        Util.NOOP
    );

    versionstampsSent.forEach(
        versionstamp -> foundationDbMessageStream.onVersionstampSent(versionstampFromInt(versionstamp)));
    versionstampsAcknowledged.forEach(
        versionstamp -> foundationDbMessageStream.handleAcknowledged(versionstampFromInt(versionstamp)));

    final List<Pair<Versionstamp, Versionstamp>> flushableRanges = foundationDbMessageStream.computeFlushableRanges();
    assertEquals(expectedRanges
            .stream()
            .map(range -> new Pair<>(versionstampFromInt(range.first()), versionstampFromInt(range.second())))
            .toList(),
        flushableRanges);
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
    buf.putLong(version);     // 8 bytes: transaction version
    buf.putShort((short) 2);   // 2 bytes: batch order within transaction
    return Versionstamp.complete(buf.array());
  }

}
