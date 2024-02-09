/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import io.vavr.Tuple2;
import org.junit.jupiter.api.Test;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class CallRoutingTableTest {

  static final CallRoutingTable basicTable = new CallRoutingTable(
      Map.of(
          (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("192.1.12.0/24"), List.of("datacenter-1", "datacenter-2", "datacenter-3"),
          (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("193.123.123.0/24"), List.of("datacenter-1", "datacenter-2"),
          (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("1.123.123.0/24"), List.of("datacenter-4")
      ),
      Map.of(
          (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0aa::/48"), List.of("datacenter-1"),
          (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0ab::/48"), List.of("datacenter-3", "datacenter-1", "datacenter-2"),
          (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0ac::/48"), List.of("datacenter-2", "datacenter-1")
      ),
      Map.of(
          new CallRoutingTable.GeoKey("SA", "SR", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3"),
          new CallRoutingTable.GeoKey("SA", "UY", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3", "datacenter-1", "datacenter-2"),
          new CallRoutingTable.GeoKey("NA", "US", Optional.of("VA"), CallRoutingTable.Protocol.v6), List.of("datacenter-2", "datacenter-1"),
          new CallRoutingTable.GeoKey("NA", "US", Optional.empty(), CallRoutingTable.Protocol.v6), List.of("datacenter-3", "datacenter-4")
      )
  );

  // has overlapping subnets
  static final CallRoutingTable overlappingTable = new CallRoutingTable(
      Map.of(
          (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("192.1.12.0/24"), List.of("datacenter-1", "datacenter-2", "datacenter-3"),
          (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("1.123.123.0/24"), List.of("datacenter-4"),
          (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("1.123.0.0/16"), List.of("datacenter-1")
      ),
      Map.of(
          (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0aa::/48"), List.of("datacenter-1"),
          (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0ac::/48"), List.of("datacenter-3", "datacenter-1", "datacenter-2"),
          (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0a0::/44"), List.of("datacenter-2", "datacenter-1")
      ),
      Map.of(
          new CallRoutingTable.GeoKey("SA", "SR", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3"),
          new CallRoutingTable.GeoKey("SA", "UY", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3", "datacenter-1", "datacenter-2"),
          new CallRoutingTable.GeoKey("NA", "US", Optional.of("VA"), CallRoutingTable.Protocol.v6), List.of("datacenter-2", "datacenter-1")
      )
  );

  @Test
  void testGetFastestDataCentersBySubnet() throws UnknownHostException {
    var v4address = Inet4Address.getByName("1.123.123.1");
    var actualV4 = basicTable.getDatacentersBySubnet(v4address);
    assertThat(actualV4).isEqualTo(List.of("datacenter-4"));

    var v6address = Inet6Address.getByName("2001:db8:b0ac:aaaa:aaaa:aaaa:aaaa:0001");
    var actualV6 = basicTable.getDatacentersBySubnet(v6address);
    assertThat(actualV6).isEqualTo(List.of("datacenter-2", "datacenter-1"));
  }

  @Test
  void testGetFastestDataCentersBySubnetOverlappingTable() throws UnknownHostException {
    var v4address = Inet4Address.getByName("1.123.123.1");
    var actualV4 = overlappingTable.getDatacentersBySubnet(v4address);
    assertThat(actualV4).isEqualTo(List.of("datacenter-4"));

    var v6address = Inet6Address.getByName("2001:db8:b0ac:aaaa:aaaa:aaaa:aaaa:0001");
    var actualV6 = overlappingTable.getDatacentersBySubnet(v6address);
    assertThat(actualV6).isEqualTo(List.of("datacenter-3", "datacenter-1", "datacenter-2"));
  }

  @Test
  void testGetFastestDataCentersByGeo() {
    var actual = basicTable.getDatacentersByGeo("SA", "SR", Optional.empty());
    assertThat(actual).isEqualTo(List.of("datacenter-3"));

    var actualWithSubdvision = basicTable.getDatacentersByGeo("NA", "US", Optional.of("VA"));
    assertThat(actualWithSubdvision).isEqualTo(List.of("datacenter-2", "datacenter-1"));
  }

  @Test
  void testGetFastestDataCentersByGeoFallback() {
    var actualExactMatch = basicTable.getDatacentersByGeo("NA", "US", Optional.of("VA"));
    assertThat(actualExactMatch).isEqualTo(List.of("datacenter-2", "datacenter-1"));

    var actualApproximateMatch = basicTable.getDatacentersByGeo("NA", "US", Optional.of("MD"));
    assertThat(actualApproximateMatch).isEqualTo(List.of("datacenter-3", "datacenter-4"));
  }

  @Test
  void testGetFastestDatacentersPrioritizesSubnet() throws UnknownHostException {
    var v4address = Inet4Address.getByName("1.123.123.1");
    var actual = basicTable.getDatacentersFor(v4address, "NA", "US", Optional.of("VA"));
    assertThat(actual).isEqualTo(List.of("datacenter-4", "datacenter-2", "datacenter-1"));
  }

  @Test
  void testGetFastestDatacentersEmptySubnet() throws UnknownHostException {
    var v4address = Inet4Address.getByName("200.200.123.1");
    var actual = basicTable.getDatacentersFor(v4address, "NA", "US", Optional.of("VA"));
    assertThat(actual).isEqualTo(List.of("datacenter-2", "datacenter-1"));
  }

  @Test
  void testGetFastestDatacentersEmptySubnetTakesExtraFromGeo() throws UnknownHostException {
    var v4address = Inet4Address.getByName("200.200.123.1");
    var actual = basicTable.getDatacentersFor(v4address, "SA", "UY", Optional.empty());
    assertThat(actual).isEqualTo(List.of("datacenter-3", "datacenter-1", "datacenter-2"));
  }

  @Test
  void testGetFastestDatacentersEmptyGeoResults() throws UnknownHostException {
    var v4address = Inet4Address.getByName("1.123.123.1");
    var actual = basicTable.getDatacentersFor(v4address, "ZZ", "AA", Optional.empty());
    assertThat(actual).isEqualTo(List.of("datacenter-4"));
  }

  @Test
  void testGetFastestDatacentersEmptyGeoTakesFromSubnet() throws UnknownHostException {
    var v4address = Inet4Address.getByName("192.1.12.1");
    var actual = basicTable.getDatacentersFor(v4address, "ZZ", "AA", Optional.empty());
    assertThat(actual).isEqualTo(List.of("datacenter-1", "datacenter-2", "datacenter-3"));
  }

  @Test
  void testGetFastestDatacentersDistinct() throws UnknownHostException {
    var v6address = Inet6Address.getByName("2001:db8:b0ac:aaaa:aaaa:aaaa:aaaa:0001");
    var actual = basicTable.getDatacentersFor(v6address, "NA", "US", Optional.of("VA"));
    assertThat(actual).isEqualTo(List.of("datacenter-2", "datacenter-1"));
  }
}
