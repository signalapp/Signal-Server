/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.eclipse.jetty.io.ArrayByteBufferPool;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.util.logging.UriInfoUtil;
import org.whispersystems.websocket.WebSocketResourceProvider;

/**
 * Gathers and reports request-level metrics for WebSocket traffic only.
 * For HTTP traffic, use {@link MetricsHttpChannelListener}.
 */
public class MetricsRequestEventListener implements RequestEventListener {

  private static final Logger logger = LoggerFactory.getLogger(MetricsRequestEventListener.class);

  private final ClientReleaseManager clientReleaseManager;

  public static final String REQUEST_COUNTER_NAME = name(MetricsRequestEventListener.class, "request");
  public static final String REQUESTS_BY_VERSION_COUNTER_NAME = name(MetricsRequestEventListener.class, "requestByVersion");
  public static final String RESPONSE_BYTES_COUNTER_NAME = name(MetricsRequestEventListener.class, "responseBytes");
  public static final String REQUEST_BYTES_COUNTER_NAME = name(MetricsRequestEventListener.class, "requestBytes");

  @VisibleForTesting
  static final String PATH_TAG = "path";

  @VisibleForTesting
  static final String METHOD_TAG = "method";

  @VisibleForTesting
  static final String STATUS_CODE_TAG = "status";

  @VisibleForTesting
  static final String TRAFFIC_SOURCE_TAG = "trafficSource";

  @VisibleForTesting
  static final String AUTHENTICATED_TAG = "authenticated";

  @VisibleForTesting
  static final String LISTEN_PORT_TAG = "listenPort";

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
        final List<Tag> tags = new ArrayList<>();
        tags.add(Tag.of(PATH_TAG, UriInfoUtil.getPathTemplate(event.getUriInfo())));
        tags.add(Tag.of(METHOD_TAG, event.getContainerRequest().getMethod()));
        tags.add(Tag.of(STATUS_CODE_TAG, String.valueOf(Optional
            .ofNullable(event.getContainerResponse())
            .map(ContainerResponse::getStatus)
            .orElse(499))));
        tags.add(Tag.of(TRAFFIC_SOURCE_TAG, trafficSource.name().toLowerCase()));
        tags.add(Tag.of(AUTHENTICATED_TAG, Optional.ofNullable(event.getContainerRequest().getProperty(WebSocketResourceProvider.REUSABLE_AUTH_PROPERTY))
            .filter(Optional.class::isInstance)
            .map(Optional.class::cast)
            .map(Optional::isPresent)
            .orElse(false)
            .toString()));

        @Nullable final String userAgent;
        {
          final List<String> userAgentValues = event.getContainerRequest().getRequestHeader(HttpHeaders.USER_AGENT);
          userAgent = userAgentValues != null && !userAgentValues.isEmpty() ? userAgentValues.getFirst() : null;
        }

        tags.addAll(UserAgentTagUtil.getLibsignalAndPlatformTags(userAgent));

        final Optional<Tag> maybeClientVersionTag =
            UserAgentTagUtil.getClientVersionTag(userAgent, clientReleaseManager);

        maybeClientVersionTag.ifPresent(tags::add);

        Optional.ofNullable(event.getContainerRequest().getProperty(WebSocketResourceProvider.LISTEN_PORT_PROPERTY))
            .filter(Integer.class::isInstance)
            .map(Integer.class::cast)
            .ifPresent(port -> tags.add(Tag.of(LISTEN_PORT_TAG, Integer.toString(port))));

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

        maybeClientVersionTag.ifPresent(clientVersionTag -> meterRegistry.counter(REQUESTS_BY_VERSION_COUNTER_NAME,
                Tags.of(clientVersionTag, UserAgentTagUtil.getPlatformTag(userAgent)))
            .increment());
      }
    }
  }
}
