/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.internal.process.MappableException;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

import java.util.ArrayList;
import java.util.List;

/**
 * Gathers and reports request-level metrics.
 */
class MetricsRequestEventListener implements RequestEventListener {

    static final  String COUNTER_NAME       = MetricRegistry.name(MetricsRequestEventListener.class, "request");
    static final  String PATH_TAG           = "path";
    static final  String STATUS_CODE_TAG    = "status";
    static final  String TRAFFIC_SOURCE_TAG = "trafficSource";

    private final TrafficSource trafficSource;
    private final MeterRegistry meterRegistry;

    public MetricsRequestEventListener(final TrafficSource trafficSource) {
        this(trafficSource, Metrics.globalRegistry);
    }

    @VisibleForTesting
    MetricsRequestEventListener(final TrafficSource trafficSource, final MeterRegistry meterRegistry) {
        this.trafficSource = trafficSource;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void onEvent(final RequestEvent event) {
        if (event.getType() == RequestEvent.Type.FINISHED) {
            if (!event.getUriInfo().getMatchedTemplates().isEmpty()) {
                final List<Tag> tags = new ArrayList<>(5);
                tags.add(Tag.of(PATH_TAG, getPathTemplate(event.getUriInfo())));
                tags.add(Tag.of(STATUS_CODE_TAG, String.valueOf(event.getContainerResponse().getStatus())));
                tags.add(Tag.of(TRAFFIC_SOURCE_TAG, trafficSource.name().toLowerCase()));

                final List<String> userAgentValues = event.getContainerRequest().getRequestHeader("User-Agent");
                tags.addAll(UserAgentTagUtil.getUserAgentTags(userAgentValues != null ? userAgentValues.stream().findFirst().orElse(null) : null));

                meterRegistry.counter(COUNTER_NAME, tags).increment();
            }
        }
    }

    @VisibleForTesting
    static String getPathTemplate(final ExtendedUriInfo uriInfo) {
        final StringBuilder pathBuilder = new StringBuilder();

        for (int i = uriInfo.getMatchedTemplates().size() - 1; i >= 0; i--) {
            pathBuilder.append(uriInfo.getMatchedTemplates().get(i).getTemplate());
        }

        return pathBuilder.toString();
    }
}
