/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

public class CallRoutingTable {
  private final TreeMap<Integer, Map<Integer, List<String>>> ipv4Map;
  private final TreeMap<Integer, Map<BigInteger, List<String>>> ipv6Map;
  private final Map<GeoKey, List<String>> geoToDatacenter;

  public CallRoutingTable(
      Map<CidrBlock.IpV4CidrBlock, List<String>> ipv4SubnetToDatacenter,
      Map<CidrBlock.IpV6CidrBlock, List<String>> ipv6SubnetToDatacenter,
      Map<GeoKey, List<String>> geoToDatacenter
  ) {
    this.ipv4Map = new TreeMap<>();
    for (Map.Entry<CidrBlock.IpV4CidrBlock, List<String>> t : ipv4SubnetToDatacenter.entrySet()) {
      if (!this.ipv4Map.containsKey(t.getKey().cidrBlockSize())) {
        this.ipv4Map.put(t.getKey().cidrBlockSize(), new HashMap<>());
      }
      this.ipv4Map
          .get(t.getKey().cidrBlockSize())
          .put(t.getKey().subnet(), t.getValue());
    }

    this.ipv6Map = new TreeMap<>();
    for (Map.Entry<CidrBlock.IpV6CidrBlock, List<String>> t : ipv6SubnetToDatacenter.entrySet()) {
      if (!this.ipv6Map.containsKey(t.getKey().cidrBlockSize())) {
        this.ipv6Map.put(t.getKey().cidrBlockSize(), new HashMap<>());
      }
      this.ipv6Map
          .get(t.getKey().cidrBlockSize())
          .put(t.getKey().subnet(), t.getValue());
    }

    this.geoToDatacenter = geoToDatacenter;
  }

  public static CallRoutingTable empty() {
    return new CallRoutingTable(Map.of(), Map.of(), Map.of());
  }

  public enum Protocol {
    v4,
    v6
  }

  public record GeoKey(
      @NotBlank String continent,
      @NotBlank String country,
      @NotNull Optional<String> subdivision,
      @NotBlank Protocol protocol
  ) {}

  /**
   * Returns ordered list of fastest datacenters based on IP & Geo info. Prioritize the results based on subnet.
   * Returns at most three, 2 by subnet and 1 by geo. Takes more from either bucket to hit 3.
   */
  public List<String> getDatacentersFor(
      InetAddress address,
      String continent,
      String country,
      Optional<String> subdivision
  ) {
    final int NUM_DATACENTERS = 3;

    if(this.isEmpty()) {
      return Collections.emptyList();
    }

    List<String> dcsBySubnet = getDatacentersBySubnet(address);
    List<String> dcsByGeo = getDatacentersByGeo(continent, country, subdivision).stream()
        .limit(NUM_DATACENTERS)
        .filter(dc ->
            (dcsBySubnet.isEmpty() || !dc.equals(dcsBySubnet.getFirst()))
                && (dcsBySubnet.size() < 2 || !dc.equals(dcsBySubnet.get(1)))
        ).toList();

    return Stream.concat(
            dcsBySubnet.stream().limit(dcsByGeo.isEmpty() ? NUM_DATACENTERS : NUM_DATACENTERS - 1),
            dcsByGeo.stream())
        .limit(NUM_DATACENTERS)
        .toList();
  }

  public boolean isEmpty() {
    return this.ipv4Map.isEmpty() && this.ipv6Map.isEmpty() && this.geoToDatacenter.isEmpty();
  }

  /**
   * Returns ordered list of fastest datacenters based on ip info. Prioritizes V4 connections.
   */
  public List<String> getDatacentersBySubnet(InetAddress address) throws IllegalArgumentException {
    if(address instanceof Inet4Address) {
      for(Map.Entry<Integer, Map<Integer, List<String>>> t:  this.ipv4Map.descendingMap().entrySet()) {
        int maskedIp = CidrBlock.IpV4CidrBlock.maskToSize((Inet4Address) address, t.getKey());
        if(t.getValue().containsKey(maskedIp)) {
          return t.getValue().get(maskedIp);
        }
      }
    } else if (address instanceof  Inet6Address) {
      for(Map.Entry<Integer, Map<BigInteger, List<String>>> t:  this.ipv6Map.descendingMap().entrySet()) {
        BigInteger maskedIp = CidrBlock.IpV6CidrBlock.maskToSize((Inet6Address) address, t.getKey());
        if(t.getValue().containsKey(maskedIp)) {
          return t.getValue().get(maskedIp);
        }
      }
    } else {
      throw new IllegalArgumentException("Expected either an Inet4Address or Inet6Address");
    }

    return Collections.emptyList();
  }

  /**
   * Returns ordered list of fastest datacenters based on geo info. Attempts to match based on subdivision, falls back
   * to country based lookup. Does not attempt to look for nearby subdivisions. Prioritizes V4 connections.
   */
  public List<String> getDatacentersByGeo(
      String continent,
      String country,
      Optional<String> subdivision
  ) {
    GeoKey v4Key = new GeoKey(continent, country, subdivision, Protocol.v4);
    List<String> v4Options = this.geoToDatacenter.getOrDefault(v4Key, Collections.emptyList());
    List<String> v4OptionsBackup = v4Options.isEmpty() && subdivision.isPresent() ?
        this.geoToDatacenter.getOrDefault(
            new GeoKey(continent, country, Optional.empty(), Protocol.v4),
            Collections.emptyList())
        : Collections.emptyList();

    GeoKey v6Key = new GeoKey(continent, country, subdivision, Protocol.v6);
    List<String> v6Options = this.geoToDatacenter.getOrDefault(v6Key, Collections.emptyList());
    List<String> v6OptionsBackup = v6Options.isEmpty() && subdivision.isPresent() ?
        this.geoToDatacenter.getOrDefault(
            new GeoKey(continent, country, Optional.empty(), Protocol.v6),
            Collections.emptyList())
        : Collections.emptyList();

    return Stream.of(
            v4Options.stream(),
            v6Options.stream(),
            v4OptionsBackup.stream(),
            v6OptionsBackup.stream()
        )
        .flatMap(Function.identity())
        .distinct()
        .toList();
  }

  public String toSummaryString() {
    return String.format(
        "[Ipv4Table=%s rows, Ipv6Table=%s rows, GeoTable=%s rows]",
        ipv4Map.size(),
        ipv6Map.size(),
        geoToDatacenter.size()
    );
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    CallRoutingTable that = (CallRoutingTable) o;
    return Objects.equals(ipv4Map, that.ipv4Map) && Objects.equals(ipv6Map, that.ipv6Map) && Objects.equals(
        geoToDatacenter, that.geoToDatacenter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ipv4Map, ipv6Map, geoToDatacenter);
  }
}
