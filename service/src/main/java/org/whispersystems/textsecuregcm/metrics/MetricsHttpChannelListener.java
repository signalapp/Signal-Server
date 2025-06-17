/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import io.dropwizard.core.setup.Environment;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.component.Container;
import org.eclipse.jetty.util.component.LifeCycle;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.util.logging.UriInfoUtil;

/**
 * Gathers and reports HTTP request metrics at the Jetty container level, which sits above Jersey. In order to get
 * templated Jersey request paths, it implements {@link jakarta.ws.rs.container.ContainerResponseFilter}, in order to give
 * itself access to the template. It is limited to {@link TrafficSource#HTTP} requests.
 * <p>
 * It implements {@link LifeCycle.Listener} without overriding methods, so that it can be an event listener that
 * Dropwizard will attach to the container&mdash;the {@link Container.Listener} implementation is where it attaches
 * itself to any {@link Connector}s.
 *
 * @see MetricsRequestEventListener
 */
public class MetricsHttpChannelListener implements HttpChannel.Listener, Container.Listener, LifeCycle.Listener,
    ContainerResponseFilter {

  private static final Logger logger = LoggerFactory.getLogger(MetricsHttpChannelListener.class);

  private record RequestInfo(String path, String method, int statusCode, @Nullable String userAgent) {

  }

  private final ClientReleaseManager clientReleaseManager;
  private final Set<String> servletPaths;

  // Use the same counter namespace as MetricsRequestEventListener for continuity
  public static final String REQUEST_COUNTER_NAME = MetricsRequestEventListener.REQUEST_COUNTER_NAME;
  public static final String REQUESTS_BY_VERSION_COUNTER_NAME = MetricsRequestEventListener.REQUESTS_BY_VERSION_COUNTER_NAME;
  @VisibleForTesting
  static final String RESPONSE_BYTES_COUNTER_NAME = MetricsRequestEventListener.RESPONSE_BYTES_COUNTER_NAME;
  @VisibleForTesting
  static final String REQUEST_BYTES_COUNTER_NAME = MetricsRequestEventListener.REQUEST_BYTES_COUNTER_NAME;
  @VisibleForTesting
  static final String URI_INFO_PROPERTY_NAME = MetricsHttpChannelListener.class.getName() + ".uriInfo";

  @VisibleForTesting
  static final String PATH_TAG = "path";

  @VisibleForTesting
  static final String METHOD_TAG = "method";

  @VisibleForTesting
  static final String STATUS_CODE_TAG = "status";

  @VisibleForTesting
  static final String TRAFFIC_SOURCE_TAG = "trafficSource";

  private final MeterRegistry meterRegistry;


  public MetricsHttpChannelListener(final ClientReleaseManager clientReleaseManager, final Set<String> servletPaths) {
    this(Metrics.globalRegistry, clientReleaseManager, servletPaths);
  }

  @VisibleForTesting
  MetricsHttpChannelListener(final MeterRegistry meterRegistry, final ClientReleaseManager clientReleaseManager,
      final Set<String> servletPaths) {
    this.meterRegistry = meterRegistry;
    this.clientReleaseManager = clientReleaseManager;
    this.servletPaths = servletPaths;
  }

  public void configure(final Environment environment) {
    // register as ContainerResponseFilter
    environment.jersey().register(this);

    // hook into lifecycle events, to react to the Connector being added
    environment.lifecycle().addEventListener(this);
  }

  @Override
  public void onRequestFailure(final Request request, final Throwable failure) {

    if (logger.isDebugEnabled()) {
      final RequestInfo requestInfo = getRequestInfo(request);

      logger.debug("Request failure: {} {} ({}) [{}] ",
          requestInfo.method(),
          requestInfo.path(),
          requestInfo.userAgent(),
          requestInfo.statusCode(), failure);
    }
  }

  @Override
  public void onResponseFailure(Request request, Throwable failure) {

    if (failure instanceof org.eclipse.jetty.io.EofException) {
      // the client disconnected early
      return;
    }

    final RequestInfo requestInfo = getRequestInfo(request);

    logger.warn("Response failure: {} {} ({}) [{}] ",
        requestInfo.method(),
        requestInfo.path(),
        requestInfo.userAgent(),
        requestInfo.statusCode(), failure);
  }

  @Override
  public void onComplete(final Request request) {

    final RequestInfo requestInfo = getRequestInfo(request);

    final List<Tag> tags = new ArrayList<>(5);
    tags.add(Tag.of(PATH_TAG, requestInfo.path()));
    tags.add(Tag.of(METHOD_TAG, requestInfo.method()));
    tags.add(Tag.of(STATUS_CODE_TAG, String.valueOf(requestInfo.statusCode())));
    tags.add(Tag.of(TRAFFIC_SOURCE_TAG, TrafficSource.HTTP.name().toLowerCase()));
    tags.addAll(UserAgentTagUtil.getLibsignalAndPlatformTags(requestInfo.userAgent()));

    meterRegistry.counter(REQUEST_COUNTER_NAME, tags).increment();

    meterRegistry.counter(RESPONSE_BYTES_COUNTER_NAME, tags).increment(request.getResponse().getContentCount());
    meterRegistry.counter(REQUEST_BYTES_COUNTER_NAME, tags).increment(request.getContentRead());

    UserAgentTagUtil.getClientVersionTag(requestInfo.userAgent(), clientReleaseManager).ifPresent(
        clientVersionTag -> meterRegistry.counter(REQUESTS_BY_VERSION_COUNTER_NAME,
            Tags.of(clientVersionTag, UserAgentTagUtil.getPlatformTag(requestInfo.userAgent()))).increment());
  }

  @Override
  public void beanAdded(final Container parent, final Object child) {
    if (child instanceof Connector connector) {
      connector.addBean(this);
    }
  }

  @Override
  public void beanRemoved(final Container parent, final Object child) {

  }

  @Override
  public void filter(final ContainerRequestContext requestContext, final ContainerResponseContext responseContext)
      throws IOException {
    requestContext.setProperty(URI_INFO_PROPERTY_NAME, requestContext.getUriInfo());
  }

  private RequestInfo getRequestInfo(Request request) {
    final String path = Optional.ofNullable(request.getAttribute(URI_INFO_PROPERTY_NAME))
        .map(attr -> UriInfoUtil.getPathTemplate((ExtendedUriInfo) attr))
        .orElseGet(() ->
            Optional.ofNullable(request.getPathInfo())
                .filter(servletPaths::contains)
                .orElse("unknown")
        );
    final String method = Optional.ofNullable(request.getMethod()).orElse("unknown");
    // Response cannot be null, but its status might not always reflect an actual response status, since it gets
    // initialized to 200
    final int status = request.getResponse().getStatus();

    @Nullable final String userAgent = request.getHeader(HttpHeaders.USER_AGENT);

    return new RequestInfo(path, method, status, userAgent);
  }

}
