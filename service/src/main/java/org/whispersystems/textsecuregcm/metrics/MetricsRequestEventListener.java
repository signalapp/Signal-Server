/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.util.logging.UriInfoUtil;
import org.whispersystems.websocket.WebSocketResourceProvider;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Gathers and reports request-level metrics for WebSocket traffic only.
 * For HTTP traffic, use {@link MetricsHttpChannelListener}.
 */
public class MetricsRequestEventListener implements RequestEventListener {

  private static final Logger logger = LoggerFactory.getLogger(MetricsRequestEventListener.class);

  private final ClientReleaseManager clientReleaseManager;

  public static final String REQUEST_COUNTER_NAME = MetricRegistry.name(MetricsRequestEventListener.class, "request");
  public static final String REQUESTS_BY_VERSION_COUNTER_NAME = MetricRegistry.name(MetricsRequestEventListener.class, "requestByVersion");
  public static final String RESPONSE_BYTES_COUNTER_NAME = MetricRegistry.name(MetricsRequestEventListener.class, "responseBytes");
  public static final String REQUEST_BYTES_COUNTER_NAME = MetricRegistry.name(MetricsRequestEventListener.class, "requestBytes");

  @VisibleForTesting
  static final String PATH_TAG = "path";

  @VisibleForTesting
  static final String METHOD_TAG = "method";

  @VisibleForTesting
  static final String STATUS_CODE_TAG = "status";

  @VisibleForTesting
  static final String TRAFFIC_SOURCE_TAG = "trafficSource";

  private final TrafficSource trafficSource;
  private final MeterRegistry meterRegistry;

  public MetricsRequestEventListener(final TrafficSource trafficSource, final ClientReleaseManager clientReleaseManager) {
    this(trafficSource, Metrics.globalRegistry, clientReleaseManager);

    if (trafficSource == TrafficSource.HTTP) {
      logger.warn("Use {} for HTTP traffic", MetricsHttpChannelListener.class.getName());
    }
  }

  @VisibleForTesting
  MetricsRequestEventListener(final TrafficSource trafficSource,
      final MeterRegistry meterRegistry,
      final ClientReleaseManager clientReleaseManager) {

    this.trafficSource = trafficSource;
    this.meterRegistry = meterRegistry;
    this.clientReleaseManager = clientReleaseManager;
  }

  @Override
  public void onEvent(final RequestEvent event) {
    if (event.getType() == RequestEvent.Type.FINISHED) {
      if (!event.getUriInfo().getMatchedTemplates().isEmpty()) {
        final List<Tag> tags = new ArrayList<>(5);
        tags.add(Tag.of(PATH_TAG, UriInfoUtil.getPathTemplate(event.getUriInfo())));
        tags.add(Tag.of(METHOD_TAG, event.getContainerRequest().getMethod()));
        tags.add(Tag.of(STATUS_CODE_TAG, String.valueOf(Optional
            .ofNullable(event.getContainerResponse())
            .map(ContainerResponse::getStatus)
            .orElse(499))));
        tags.add(Tag.of(TRAFFIC_SOURCE_TAG, trafficSource.name().toLowerCase()));

        @Nullable final String userAgent;
        {
          final List<String> userAgentValues = event.getContainerRequest().getRequestHeader(HttpHeaders.USER_AGENT);
          userAgent = userAgentValues != null && !userAgentValues.isEmpty() ? userAgentValues.get(0) : null;
        }

        tags.addAll(UserAgentTagUtil.getLibsignalAndPlatformTags(userAgent));

        meterRegistry.counter(REQUEST_COUNTER_NAME, tags).increment();

        Optional.ofNullable(event.getContainerRequest().getProperty(WebSocketResourceProvider.REQUEST_LENGTH_PROPERTY))
            .filter(Integer.class::isInstance)
            .map(Integer.class::cast)
            .filter(bytes -> bytes >= 0)
            .ifPresent(bytes -> meterRegistry.counter(REQUEST_BYTES_COUNTER_NAME, tags).increment(bytes));

        Optional.ofNullable(event.getContainerRequest().getProperty(WebSocketResourceProvider.RESPONSE_LENGTH_PROPERTY))
            .filter(Integer.class::isInstance)
            .map(Integer.class::cast)
            .filter(bytes -> bytes >= 0)
            .ifPresent(bytes -> meterRegistry.counter(RESPONSE_BYTES_COUNTER_NAME, tags).increment(bytes));

        UserAgentTagUtil.getClientVersionTag(userAgent, clientReleaseManager)
            .ifPresent(clientVersionTag -> meterRegistry.counter(REQUESTS_BY_VERSION_COUNTER_NAME,
                    Tags.of(clientVersionTag, UserAgentTagUtil.getPlatformTag(userAgent)))
                .increment());
      }
    }
  }
}
