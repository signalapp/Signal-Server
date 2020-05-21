package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

import java.util.ArrayList;
import java.util.List;

/**
 * Gathers and reports request-level metrics.
 */
class MetricsRequestEventListener implements RequestEventListener {

    static final         String         COUNTER_NAME       = MetricRegistry.name(MetricsRequestEventListener.class, "request");

    static final         String         PATH_TAG           = "path";
    static final         String         STATUS_CODE_TAG    = "status";

    private final        MeterRegistry  meterRegistry;

    public MetricsRequestEventListener() {
        this(Metrics.globalRegistry);
    }

    @VisibleForTesting
    MetricsRequestEventListener(final MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void onEvent(final RequestEvent event) {
        if (event.getType() == RequestEvent.Type.FINISHED) {
            if (!event.getUriInfo().getMatchedTemplates().isEmpty()) {
                final List<Tag> tags = new ArrayList<>(4);
                tags.add(Tag.of(PATH_TAG, getPathTemplate(event.getUriInfo())));
                tags.add(Tag.of(STATUS_CODE_TAG, String.valueOf(event.getContainerResponse().getStatus())));

                event.getContainerRequest().getRequestHeader("User-Agent")
                                           .stream()
                                           .findFirst()
                                           .map(UserAgentTagUtil::getUserAgentTags)
                                           .ifPresent(tags::addAll);

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
