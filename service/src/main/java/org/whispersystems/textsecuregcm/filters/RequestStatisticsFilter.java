/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.requireNonNull;

import com.google.common.net.InetAddresses;
import io.micrometer.core.instrument.Metrics;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.TrafficSource;

public class RequestStatisticsFilter implements ContainerRequestFilter {

  private static final Logger logger = LoggerFactory.getLogger(RequestStatisticsFilter.class);

  private static final String CONTENT_LENGTH_DISTRIBUTION_NAME = name(RequestStatisticsFilter.class, "contentLength");

  private static final String IP_VERSION_METRIC = MetricsUtil.name(RequestStatisticsFilter.class, "ipVersion");

  private static final String TRAFFIC_SOURCE_TAG = "trafficSource";

  private static final String IP_VERSION_TAG = "ipVersion";

  @Nonnull
  private final String trafficSourceTag;


  public RequestStatisticsFilter(@Nonnull final TrafficSource trafficeSource) {
    this.trafficSourceTag = requireNonNull(trafficeSource).name().toLowerCase();
  }

  @Override
  public void filter(final ContainerRequestContext requestContext) throws IOException {
    try {
      Metrics.summary(CONTENT_LENGTH_DISTRIBUTION_NAME, TRAFFIC_SOURCE_TAG, trafficSourceTag)
          .record(requestContext.getLength());
      Metrics.counter(IP_VERSION_METRIC, TRAFFIC_SOURCE_TAG, trafficSourceTag, IP_VERSION_TAG,
              resolveIpVersion(requestContext))
          .increment();
    } catch (final Exception e) {
      logger.warn("Error recording request statistics", e);
    }
  }

  @Nonnull
  private static String resolveIpVersion(@Nonnull final ContainerRequestContext ctx) {
    final String ipString = (String) ctx.getProperty(RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME);

    try {
      final InetAddress addr = InetAddresses.forString(ipString);
      if (addr instanceof Inet4Address) {
        return "IPv4";
      }
      if (addr instanceof Inet6Address) {
        return "IPv6";
      }
    } catch (IllegalArgumentException e) {
      // ignore illegal argument exception
    }
    return "unresolved";
  }
}
