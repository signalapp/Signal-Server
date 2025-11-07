/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.asn;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code AsnInfoProvider} implementation that supports both IPv4 and IPv6.
 */
public class AsnInfoProviderImpl implements AsnInfoProvider {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Nonnull
  private final NavigableMap<Long, AsnRange<Long>> asnBlocksByFirstIpv4;

  @Nonnull
  private final NavigableMap<BigInteger, AsnRange<BigInteger>> asnBlocksByFirstIpv6;


  /**
   * Creates an instance of {@code AsnInfoProviderImpl} using data from <a href="https://iptoasn.com/">iptoasn.com</a>.
   * @param tsvGzInputStream gzip input stream representing the data.
   */
  @Nonnull
  public static AsnInfoProviderImpl fromTsvGz(@Nonnull final InputStream tsvGzInputStream) {
    try (final GZIPInputStream inputStream = new GZIPInputStream(tsvGzInputStream)) {
      return fromTsv(inputStream);
    } catch (final IOException e) {
      log.error("failed to ungzip the input stream", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates an instance of {@code AsnInfoProviderImpl} using data from <a href="https://iptoasn.com/">iptoasn.com</a>.
   * @param tsvInputStream input stream representing the data.
   */
  @Nonnull
  public static AsnInfoProviderImpl fromTsv(@Nonnull final InputStream tsvInputStream) {
    try (final InputStreamReader tsvReader = new InputStreamReader(tsvInputStream)) {
      final NavigableMap<Long, AsnRange<Long>> ip4asns = new TreeMap<>();
      final NavigableMap<BigInteger, AsnRange<BigInteger>> ip6asns = new TreeMap<>();
      final Map<Long, AsnInfo> asnInfoCache = new HashMap<>();

      try (final CSVParser csvParser = CSVFormat.TDF.parse(tsvReader)) {
        for (final CSVRecord csvRecord : csvParser) {
          // format:
          // range_start_ip_string range_end_ip_string AS_number country_code AS_description
          final InetAddress startIp = InetAddress.getByName(csvRecord.get(0));
          final InetAddress endIp = InetAddress.getByName(csvRecord.get(1));
          final long asn = Long.parseLong(csvRecord.get(2));
          final String regionCode = csvRecord.get(3);
          // country code should be the same for any ASN, so we're caching AsnInfo objects
          // not to have multiple instances with the same values
          final AsnInfo asnInfo = asnInfoCache.computeIfAbsent(asn, _ -> new AsnInfo(asn, regionCode));
          if (!regionCode.equals(asnInfo.regionCode())) {
            log.warn("ASN {} mapped to country codes {} and {}", asn, regionCode, asnInfo.regionCode());
          }

          // IPv4
          if (startIp instanceof Inet4Address inet4address) {
            final AsnRange<Long> asnRange = new AsnRange<>(
                ip4BytesToLong(inet4address),
                ip4BytesToLong((Inet4Address) endIp),
                asnInfo
            );
            ip4asns.put(asnRange.from(), asnRange);
          }

          // IPv6
          if (startIp instanceof Inet6Address inet6address) {
            final AsnRange<BigInteger> asnRange = new AsnRange<>(
                ip6BytesToBigInteger(inet6address),
                ip6BytesToBigInteger((Inet6Address) endIp),
                asnInfo
            );
            ip6asns.put(asnRange.from(), asnRange);
          }
        }
      }
      return new AsnInfoProviderImpl(ip4asns, ip6asns);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public AsnInfoProviderImpl(
      @Nonnull final NavigableMap<Long, AsnRange<Long>> asnBlocksByFirstIpv4,
      @Nonnull final NavigableMap<BigInteger, AsnRange<BigInteger>> asnBlocksByFirstIpv6) {
    this.asnBlocksByFirstIpv4 = requireNonNull(asnBlocksByFirstIpv4);
    this.asnBlocksByFirstIpv6 = requireNonNull(asnBlocksByFirstIpv6);
  }

  @Nonnull
  @Override
  public Optional<AsnInfo> lookup(@Nonnull final String ipString) {
    try {
      final InetAddress address = InetAddress.getByName(ipString);
      if (address instanceof Inet4Address ip4) {
        final Long key = ip4BytesToLong(ip4);
        return lookupInMap(asnBlocksByFirstIpv4, key);
      }
      if (address instanceof Inet6Address ip6) {
        final BigInteger key = ip6BytesToBigInteger(ip6);
        return lookupInMap(asnBlocksByFirstIpv6, key);
      }
      // safety net, should never happen
      log.warn("Unknown InetAddress implementation: {}", address.getClass().getName());
    } catch (final Exception _) {
      log.error("Could not resolve ASN for IP string {}", ipString);
    }
    return Optional.empty();
  }

  @VisibleForTesting
  protected static long ip4BytesToLong(@Nonnull final Inet4Address address) {
    final byte[] arr = address.getAddress();
    Validate.isTrue(arr.length == 4);
    return Integer.toUnsignedLong(ByteBuffer.wrap(arr).getInt());
  }

  @VisibleForTesting
  protected static BigInteger ip6BytesToBigInteger(@Nonnull final Inet6Address address) {
    final byte[] arr = address.getAddress();
    Validate.isTrue(arr.length == 16);
    return new BigInteger(1, arr);
  }

  @Nonnull
  private static <T extends Comparable<T>> Optional<AsnInfo> lookupInMap(
      @Nonnull final NavigableMap<T, AsnRange<T>> map,
      @Nonnull final T key) {
    return Optional.ofNullable(map.floorEntry(key))
        .filter(e -> e.getValue().contains(key) && e.getValue().asnInfo().asn() != 0)
        .map(e -> e.getValue().asnInfo());
  }
}
