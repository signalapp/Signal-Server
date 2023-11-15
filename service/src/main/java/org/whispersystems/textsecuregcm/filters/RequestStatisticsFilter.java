/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.requireNonNull;

import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.TrafficSource;

public class RequestStatisticsFilter implements ContainerRequestFilter {

  private static final Logger logger = LoggerFactory.getLogger(RequestStatisticsFilter.class);

  private static final String CONTENT_LENGTH_DISTRIBUTION_NAME = name(RequestStatisticsFilter.class, "contentLength");

  private static final String TRAFFIC_SOURCE_TAG = "trafficSource";

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
    } catch (final Exception e) {
      logger.warn("Error recording request statistics", e);
    }
  }
}
