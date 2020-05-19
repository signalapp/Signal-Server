package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.whispersystems.textsecuregcm.util.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Gathers and reports request-level metrics.
 */
class MetricsRequestEventListener implements RequestEventListener {

    static final         int                       MAX_VERSIONS       = 10_000;

    static final         String                    COUNTER_NAME       = MetricRegistry.name(MetricsRequestEventListener.class, "request");

    static final         String                    PATH_TAG           = "path";
    static final         String                    STATUS_CODE_TAG    = "status";
    static final         String                    PLATFORM_TAG       = "platform";
    static final         String                    VERSION_TAG        = "clientVersion";

    static final         List<Tag>                 OVERFLOW_TAGS      = List.of(Tag.of(PLATFORM_TAG, "overflow"), Tag.of(VERSION_TAG, "overflow"));
    static final         List<Tag>                 UNRECOGNIZED_TAGS  = List.of(Tag.of(PLATFORM_TAG, "unrecognized"), Tag.of(VERSION_TAG, "unrecognized"));

    private static final Pattern                   USER_AGENT_PATTERN = Pattern.compile("Signal-([^ ]+) ([^ ]+).*$", Pattern.CASE_INSENSITIVE);

    private final        MeterRegistry             meterRegistry;
    private final        Set<Pair<String, String>> seenVersions       = new HashSet<>();

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
                                           .map(this::getUserAgentTags)
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

    @VisibleForTesting
    List<Tag> getUserAgentTags(final String userAgent) {
        final Matcher matcher = USER_AGENT_PATTERN.matcher(userAgent);
        final List<Tag> tags;

        if (matcher.matches()) {
            final Pair<String, String> platformAndVersion = new Pair<>(matcher.group(1).toLowerCase(), matcher.group(2));

            final boolean allowVersion;

            synchronized (seenVersions) {
                allowVersion = seenVersions.contains(platformAndVersion) || (seenVersions.size() < MAX_VERSIONS && seenVersions.add(platformAndVersion));
            }

            tags = allowVersion ? List.of(Tag.of(PLATFORM_TAG, platformAndVersion.first()), Tag.of(VERSION_TAG, platformAndVersion.second())) : OVERFLOW_TAGS;
        } else {
            tags = UNRECOGNIZED_TAGS;
        }

        return tags;
    }
}
