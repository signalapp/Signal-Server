/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Util;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.function.Supplier;
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

  public TurnCallRouter(
      @Nonnull Supplier<CallDnsRecords> callDnsRecords,
      @Nonnull Supplier<CallRoutingTable> performanceRouting,
      @Nonnull Supplier<CallRoutingTable> manualRouting,
      @Nonnull DynamicConfigTurnRouter configTurnRouter,
      @Nonnull Supplier<DatabaseReader> geoIp
  ) {
    this.performanceRouting = performanceRouting;
    this.callDnsRecords = callDnsRecords;
    this.manualRouting = manualRouting;
    this.configTurnRouter = configTurnRouter;
    this.geoIp = geoIp;
  }

  /**
   * Gets Turn Instance addresses. Returns both the IPv4 and IPv6 addresses. Prioritizes V4 connections.
   * @param aci aci of client
   * @param clientAddress IP address to base routing on
   * @param instanceLimit max instances to return options for
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
      return new TurnServerOptions(this.configTurnRouter.getHostname(), null, this.configTurnRouter.randomUrls());
    }
  }

  TurnServerOptions getRoutingForInner(
      @Nonnull final UUID aci,
      @Nonnull final Optional<InetAddress> clientAddress,
      final int instanceLimit
  ) {
    if (instanceLimit < 1) {
      throw new IllegalArgumentException("Limit cannot be less than one");
    }

    String hostname = this.configTurnRouter.getHostname();

    List<String> targetedUrls = this.configTurnRouter.targetedUrls(aci);
    if(!targetedUrls.isEmpty()) {
      return new TurnServerOptions(hostname, null, targetedUrls);
    }

    if(clientAddress.isEmpty() || this.configTurnRouter.shouldRandomize()) {
      return new TurnServerOptions(hostname, null, this.configTurnRouter.randomUrls());
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
    List<String> urlsWithIps = getUrlsForInstances(selectInstances(datacenters, instanceLimit));
    return new TurnServerOptions(hostname, urlsWithIps, this.configTurnRouter.randomUrls());
  }

  private List<String> selectInstances(List<String> datacenters, int limit) {
    if(datacenters.isEmpty() || limit == 0) {
      return Collections.emptyList();
    }

    CallDnsRecords dnsRecords = this.callDnsRecords.get();
    List<InetAddress> ipv4Selection = datacenters.stream()
        .flatMap(dc -> Util.randomNOfStable(dnsRecords.aByRegion().get(dc), 2).stream())
        .toList();
    List<InetAddress> ipv6Selection = datacenters.stream()
        .flatMap(dc -> Util.randomNOfStable(dnsRecords.aaaaByRegion().get(dc), 2).stream())
        .toList();
    if (ipv4Selection.size() < ipv6Selection.size()) {
      ipv4Selection = ipv4Selection.stream().limit(limit / 2).toList();
      ipv6Selection = ipv6Selection.stream().limit(limit - ipv4Selection.size()).toList();
    } else {
      ipv6Selection = ipv6Selection.stream().limit(limit / 2).toList();
      ipv4Selection = ipv4Selection.stream().limit(limit - ipv6Selection.size()).toList();
    }

    return Stream.concat(
        ipv4Selection.stream().map(InetAddress::getHostAddress),
        // map ipv6 to RFC3986 format i.e. surrounded by brackets
        ipv6Selection.stream().map(i -> String.format("[%s]", i.getHostAddress()))
    ).toList();
  }

  private static List<String> getUrlsForInstances(List<String> instanceIps) {
    return instanceIps.stream().flatMap(ip -> Stream.of(
            String.format("stun:%s", ip),
            String.format("turn:%s", ip),
            String.format("turn:%s:80?transport=tcp", ip),
            String.format("turns:%s:443?transport=tcp", ip)
        )
    ).toList();
  }
}
