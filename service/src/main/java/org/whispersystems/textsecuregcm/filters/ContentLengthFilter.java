/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static com.codahale.metrics.MetricRegistry.name;

import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.TrafficSource;

public class ContentLengthFilter implements ContainerRequestFilter {

  private static final String CONTENT_LENGTH_DISTRIBUTION_NAME = name(ContentLengthFilter.class, "contentLength");

  private static final Logger logger = LoggerFactory.getLogger(ContentLengthFilter.class);
  private final TrafficSource trafficSource;

  public ContentLengthFilter(final TrafficSource trafficeSource) {
    this.trafficSource = trafficeSource;
  }

  @Override
  public void filter(final ContainerRequestContext requestContext) throws IOException {
    try {
      Metrics.summary(CONTENT_LENGTH_DISTRIBUTION_NAME, "trafficSource", trafficSource.name().toLowerCase()).record(requestContext.getLength());
    } catch (final Exception e) {
      logger.warn("Error recording content length", e);
    }
  }
}
