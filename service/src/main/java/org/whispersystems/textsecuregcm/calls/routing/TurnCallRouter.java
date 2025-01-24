/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Util;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Returns routes based on performance tables, manually routing tables, and target routing. Falls back to a random Turn
 * instance that the server knows about.
 */
public class TurnCallRouter {

  private final Logger logger = LoggerFactory.getLogger(TurnCallRouter.class);

  private final Supplier<CallDnsRecords> callDnsRecords;
  private final Supplier<CallRoutingTable> performanceRouting;
  private final Supplier<CallRoutingTable> manualRouting;
  private final DynamicConfigTurnRouter configTurnRouter;
  private final Supplier<DatabaseReader> geoIp;
  // controls whether instance IPs are shuffled. using if & boolean is ~5x faster than a function pointer
  private final boolean stableSelect;

  public TurnCallRouter(
      @Nonnull Supplier<CallDnsRecords> callDnsRecords,
      @Nonnull Supplier<CallRoutingTable> performanceRouting,
      @Nonnull Supplier<CallRoutingTable> manualRouting,
      @Nonnull DynamicConfigTurnRouter configTurnRouter,
      @Nonnull Supplier<DatabaseReader> geoIp,
      boolean stableSelect
  ) {
    this.performanceRouting = performanceRouting;
    this.callDnsRecords = callDnsRecords;
    this.manualRouting = manualRouting;
    this.configTurnRouter = configTurnRouter;
    this.geoIp = geoIp;
    this.stableSelect = stableSelect;
  }

  public TurnServerOptions getRoutingFor(
      @Nonnull final UUID aci,
      @Nonnull final Optional<InetAddress> clientAddress
  ) {
    return getRoutingFor(aci, clientAddress,  this.configTurnRouter.getDefaultInstanceIpCount());
  }

  /**
   * Gets Turn Instance addresses. Returns both the IPv4 and IPv6 addresses. Prefers to match the IP protocol of the
   * client address in datacenter selection. Returns 2 instance options of the preferred protocol for every one instance
   * of the other.
   * @param aci aci of client
   * @param clientAddress IP address to base routing on
   * @param instanceLimit max instances to return options for
   * @return Up to two * instanceLimit options, half in ipv4, half in ipv6
   */
  public TurnServerOptions getRoutingFor(
      @Nonnull final UUID aci,
      @Nonnull final Optional<InetAddress> clientAddress,
      final int instanceLimit
  ) {
    try {
      return getRoutingForInner(aci, clientAddress, instanceLimit);
    } catch(Exception e) {
      logger.error("Failed to perform routing", e);
      return new TurnServerOptions(this.configTurnRouter.getHostname(), null, Optional.of(this.configTurnRouter.randomUrls()));
    }
  }

  TurnServerOptions getRoutingForInner(
      @Nonnull final UUID aci,
      @Nonnull final Optional<InetAddress> clientAddress,
      final int instanceLimit
  ) {
    String hostname = this.configTurnRouter.getHostname();

    List<String> targetedUrls = this.configTurnRouter.targetedUrls(aci);
    if(!targetedUrls.isEmpty()) {
      return new TurnServerOptions(hostname, Optional.empty(), Optional.ofNullable(targetedUrls));
    }

    if(clientAddress.isEmpty() || this.configTurnRouter.shouldRandomize() || instanceLimit < 1) {
      return new TurnServerOptions(hostname, Optional.empty(), Optional.ofNullable(this.configTurnRouter.randomUrls()));
    }

    CityResponse geoInfo;
    try {
      geoInfo = geoIp.get().city(clientAddress.get());
    } catch (IOException | GeoIp2Exception e) {
      throw new RuntimeException(e);
    }
    Optional<String> subdivision = !geoInfo.getSubdivisions().isEmpty()
        ? Optional.of(geoInfo.getSubdivisions().getFirst().getIsoCode())
        : Optional.empty();

    List<String> datacenters = this.manualRouting.get().getDatacentersFor(
        clientAddress.get(),
        geoInfo.getContinent().getCode(),
        geoInfo.getCountry().getIsoCode(),
        subdivision
    );

    if (datacenters.isEmpty()){
      datacenters = this.performanceRouting.get().getDatacentersFor(
          clientAddress.get(),
          geoInfo.getContinent().getCode(),
          geoInfo.getCountry().getIsoCode(),
          subdivision
      );
    }

    List<String> urlsWithIps = getUrlsForInstances(
        selectInstances(
            datacenters,
            instanceLimit
        ));
    return new TurnServerOptions(hostname, Optional.of(urlsWithIps), Optional.of(minimalRandomUrls()));
  }

  // Includes only the udp options in the randomUrls
  private List<String> minimalRandomUrls(){
    return this.configTurnRouter.randomUrls().stream()
        .filter(s -> s.startsWith("turn:") && !s.endsWith("transport=tcp"))
        .toList();
  }

  // returns balanced number of instances across provided datacenters, prioritizing the datacenters earlier in the list
  private List<String> selectInstances(List<String> datacenters, int instanceLimit) {
    if(datacenters.isEmpty() || instanceLimit == 0) {
      return Collections.emptyList();
    }

    CallDnsRecords dnsRecords = this.callDnsRecords.get();
    List<List<InetAddress>> ipv4Options = datacenters.stream()
        .map(dc -> randomNOf(dnsRecords.aByRegion().get(dc), instanceLimit, stableSelect))
        .toList();
    List<List<InetAddress>> ipv6Options = datacenters.stream()
        .map(dc -> randomNOf(dnsRecords.aaaaByRegion().get(dc), instanceLimit, stableSelect))
        .toList();

    List<InetAddress> ipv4Selection = selectFromOptions(ipv4Options, instanceLimit);
    List<InetAddress> ipv6Selection = selectFromOptions(ipv6Options, instanceLimit);

    return Stream.concat(
        ipv4Selection.stream().map(InetAddress::getHostAddress),
        // map ipv6 to RFC3986 format i.e. surrounded by brackets
        ipv6Selection.stream().map(i -> String.format("[%s]", i.getHostAddress()))
    ).toList();
  }

  private static List<InetAddress> selectFromOptions(List<List<InetAddress>> recordsByDc, int instanceLimit) {
    return IntStream.range(0, recordsByDc.size())
        .mapToObj(dcIndex -> IntStream.range(0, recordsByDc.get(dcIndex).size())
          .mapToObj(addressIndex -> Triple.of(addressIndex, dcIndex, recordsByDc.get(dcIndex).get(addressIndex))))
        .flatMap(i -> i)
        .sorted(Comparator.comparingInt((Triple<Integer, Integer, InetAddress> t) -> t.getLeft())
            .thenComparingInt(Triple::getMiddle))
        .limit(instanceLimit)
        .sorted(Comparator.comparingInt(Triple::getMiddle))
        .map(Triple::getRight)
        .toList();
  }

  private static <E> List<E> randomNOf(List<E> values, int n, boolean stableSelect) {
    return stableSelect ? Util.randomNOfStable(values, n) : Util.randomNOfShuffled(values, n);
  }

  private static List<String> getUrlsForInstances(List<String> instanceIps) {
    return instanceIps.stream().flatMap(ip -> Stream.of(
            String.format("turn:%s", ip),
            String.format("turn:%s:80?transport=tcp", ip),
            String.format("turns:%s:443?transport=tcp", ip)
        )
    ).toList();
  }
}
