/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;


final class CallRoutingTableParser {

  private final static int IPV4_DEFAULT_BLOCK_SIZE = 24;
  private final static int IPV6_DEFAULT_BLOCK_SIZE = 48;
  private static final ObjectMapper objectMapper = JsonMapper.builder()
      .enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
      .build();

  /** Used for parsing JSON */
  private static class RawCallRoutingTable {
      public Map<String, List<String>> ipv4GeoToDataCenters = Map.of();
      public Map<String, List<String>> ipv6GeoToDataCenters = Map.of();
      public Map<String, List<String>> ipv4SubnetsToDatacenters = Map.of();
      public Map<String, List<String>> ipv6SubnetsToDatacenters = Map.of();
  }

  private final static String WHITESPACE_REGEX = "\\s+";

  public static CallRoutingTable fromJson(final Reader inputReader) throws IOException {
    try (final BufferedReader reader = new BufferedReader(inputReader)) {
      RawCallRoutingTable rawTable = objectMapper.readValue(reader, RawCallRoutingTable.class);

      Map<CidrBlock.IpV4CidrBlock, List<String>> ipv4SubnetToDatacenter = rawTable.ipv4SubnetsToDatacenters
          .entrySet()
          .stream()
          .collect(Collectors.toUnmodifiableMap(
              e -> (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock(e.getKey(), IPV4_DEFAULT_BLOCK_SIZE),
                  Map.Entry::getValue
          ));

      Map<CidrBlock.IpV6CidrBlock, List<String>> ipv6SubnetToDatacenter = rawTable.ipv6SubnetsToDatacenters
          .entrySet()
          .stream()
          .collect(Collectors.toUnmodifiableMap(
              e -> (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock(e.getKey(), IPV6_DEFAULT_BLOCK_SIZE),
              Map.Entry::getValue
          ));

      Map<CallRoutingTable.GeoKey, List<String>> geoToDatacenter = Stream.concat(
          rawTable.ipv4GeoToDataCenters
              .entrySet()
              .stream()
              .map(e -> Map.entry(parseRawGeoKey(e.getKey(), CallRoutingTable.Protocol.v4), e.getValue())),
          rawTable.ipv6GeoToDataCenters
              .entrySet()
              .stream()
              .map(e -> Map.entry(parseRawGeoKey(e.getKey(), CallRoutingTable.Protocol.v6), e.getValue()))
      ).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

      return new CallRoutingTable(
          ipv4SubnetToDatacenter,
          ipv6SubnetToDatacenter,
          geoToDatacenter
      );
    }
  }

  private static CallRoutingTable.GeoKey parseRawGeoKey(String rawKey, CallRoutingTable.Protocol protocol) {
    String[] splits = rawKey.split("-");
    if (splits.length < 2 || splits.length > 3) {
      throw new IllegalArgumentException("Invalid raw key");
    }

    Optional<String> subdivision = splits.length < 3 ? Optional.empty() : Optional.of(splits[2]);
    return new CallRoutingTable.GeoKey(splits[0], splits[1], subdivision, protocol);
  }

  /**
   * Parses a call routing table in TSV format. Example below - see tests for more examples:
   192.0.2.0/24 northamerica-northeast1
   198.51.100.0/24 us-south1
   203.0.113.0/24 asia-southeast1

   2001:db8:b0a9::/48 us-east4
   2001:db8:b0f5::/48 us-central1 northamerica-northeast1 us-east4
   2001:db8:9406::/48 us-east1 us-central1

   SA-SR-v4 us-east1 us-east4
   SA-SR-v6 us-east1 us-south1
   SA-UY-v4 southamerica-west1 southamerica-east1 europe-west3
   SA-UY-v6 southamerica-west1 europe-west4
   SA-VE-v4 us-east1 us-east4 us-south1
   SA-VE-v6 us-east1 northamerica-northeast1 us-east4
   ZZ-ZZ-v4 asia-south1 europe-southwest1 australia-southeast1
   */
  public static CallRoutingTable fromTsv(final Reader inputReader) throws IOException {
    try (final BufferedReader reader = new BufferedReader(inputReader)) {
      // use maps to silently dedupe CidrBlocks
      Map<CidrBlock.IpV4CidrBlock, List<String>> ipv4Map = new HashMap<>();
      Map<CidrBlock.IpV6CidrBlock, List<String>> ipv6Map = new HashMap<>();
      Map<CallRoutingTable.GeoKey, List<String>> ipGeoTable = new HashMap<>();
      String line;
      while((line = reader.readLine()) != null) {
        if(line.isBlank()) {
          continue;
        }

        List<String> splits = Arrays.stream(line.split(WHITESPACE_REGEX)).filter(s -> !s.isBlank()).toList();
        if (splits.size() < 2) {
          throw new IllegalStateException("Invalid row, expected some key and list of values");
        }

        List<String> datacenters = splits.subList(1, splits.size());
        switch (guessLineType(splits)) {
          case v4 ->  {
            CidrBlock cidrBlock = CidrBlock.parseCidrBlock(splits.getFirst());
            if(!(cidrBlock instanceof CidrBlock.IpV4CidrBlock)) {
              throw new IllegalArgumentException("Expected an ipv4 cidr block");
            }
            ipv4Map.put((CidrBlock.IpV4CidrBlock) cidrBlock, datacenters);
          }
          case v6 -> {
            CidrBlock cidrBlock = CidrBlock.parseCidrBlock(splits.getFirst());
            if(!(cidrBlock instanceof CidrBlock.IpV6CidrBlock)) {
              throw new IllegalArgumentException("Expected an ipv6 cidr block");
            }
            ipv6Map.put((CidrBlock.IpV6CidrBlock) cidrBlock, datacenters);
          }
          case Geo -> {
            String[] geo = splits.getFirst().split("-");
            if(geo.length < 3) {
              throw new IllegalStateException("Geo row key invalid, expected atleast continent, country, and protocol");
            }
            String continent = geo[0];
            String country = geo[1];
            Optional<String> subdivision = geo.length > 3 ? Optional.of(geo[2]) : Optional.empty();
            CallRoutingTable.Protocol protocol = CallRoutingTable.Protocol.valueOf(geo[geo.length - 1].toLowerCase());
            CallRoutingTable.GeoKey tableKey = new CallRoutingTable.GeoKey(
                continent,
                country,
                subdivision,
                protocol
            );
            ipGeoTable.put(tableKey, datacenters);
          }
        }
      }

      return new CallRoutingTable(
          ipv4Map,
          ipv6Map,
          ipGeoTable
      );
    }
  }

  private static LineType guessLineType(List<String> splits) {
    String first = splits.getFirst();
    if (first.contains("-")) {
      return LineType.Geo;
    } else if(first.contains(":")) {
      return LineType.v6;
    } else if (first.contains(".")) {
      return LineType.v4;
    }

    throw new IllegalArgumentException(String.format("Invalid line, could not determine type from '%s'", first));
  }

  private enum LineType {
    v4, v6, Geo
  }
}
