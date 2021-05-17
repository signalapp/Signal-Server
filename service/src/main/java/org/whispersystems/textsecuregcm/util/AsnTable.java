/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.Reader;
import java.net.Inet4Address;
import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Allows IP->ASN lookup operations using data from https://iptoasn.com/.
 */
class AsnTable {
  private final NavigableMap<Long, AsnRange> asnBlocksByFirstIp;

  private static class AsnRange {
    private final long rangeStart;
    private final long rangeEnd;

    private final long asn;

    private AsnRange(long rangeStart, long rangeEnd, long asn) {
      this.rangeStart = rangeStart;
      this.rangeEnd = rangeEnd;
      this.asn = asn;
    }

    boolean contains(final long address) {
      return address >= rangeStart && address <= rangeEnd;
    }

    long getAsn() {
      return asn;
    }
  }

  public static final AsnTable EMPTY = new AsnTable();

  public AsnTable(final Reader tsvReader) throws IOException {
    final TreeMap<Long, AsnRange> treeMap = new TreeMap<>();

    try (final CSVParser csvParser = CSVFormat.TDF.parse(tsvReader)) {
      for (final CSVRecord record : csvParser) {
        final long start = Long.parseLong(record.get(0), 10);
        final long end = Long.parseLong(record.get(1), 10);
        final long asn = Long.parseLong(record.get(2), 10);

        treeMap.put(start, new AsnRange(start, end, asn));
      }
    }

    asnBlocksByFirstIp = treeMap;
  }

  private AsnTable() {
    asnBlocksByFirstIp = new TreeMap<>();
  }

  public Optional<Long> getAsn(final Inet4Address address) {
    final long addressAsLong = ipToLong(address);

    return Optional.ofNullable(asnBlocksByFirstIp.floorEntry(addressAsLong))
        .filter(entry -> entry.getValue().contains(addressAsLong))
        .map(entry -> entry.getValue().getAsn());
  }

  @VisibleForTesting
  static long ipToLong(final Inet4Address address) {
    final ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.position(4);
    buffer.put(address.getAddress());

    buffer.flip();
    return buffer.getLong();
  }
}
