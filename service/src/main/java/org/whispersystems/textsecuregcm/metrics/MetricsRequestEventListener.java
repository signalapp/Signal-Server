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
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Gathers and reports request-level metrics.
 */
class MetricsRequestEventListener implements RequestEventListener {

    static final  String REQUEST_COUNTER_NAME = MetricRegistry.name(MetricsRequestEventListener.class, "request");
    static final  String PATH_TAG             = "path";
    static final  String STATUS_CODE_TAG      = "status";
    static final  String TRAFFIC_SOURCE_TAG   = "trafficSource";

    static final String ANDROID_REQUEST_COUNTER_NAME = MetricRegistry.name(MetricsRequestEventListener.class, "androidRequest");
    static final String DESKTOP_REQUEST_COUNTER_NAME = MetricRegistry.name(MetricsRequestEventListener.class, "desktopRequest");
    static final String OS_TAG                       = "os";
    static final String SDK_TAG                      = "sdkVersion";

    private static final Set<String> ACCEPTABLE_DESKTOP_OS_STRINGS = Set.of("linux", "macos", "windows");

    private static final String ANDROID_SDK_PREFIX      = "Android/";
    private static final int    MIN_ANDROID_SDK_VERSION = 19;
    private static final int    MAX_ANDROID_SDK_VERSION = 50;

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

                meterRegistry.counter(REQUEST_COUNTER_NAME, tags).increment();

                try {
                    final UserAgent userAgent = UserAgentUtil.parseUserAgentString(userAgentValues != null ? userAgentValues.stream().findFirst().orElse(null) : null);

                    recordDesktopOperatingSystem(userAgent);
                    recordAndroidSdkVersion(userAgent);
                } catch (final UnrecognizedUserAgentException ignored) {
                }
            }
        }
    }

    @VisibleForTesting
    void recordDesktopOperatingSystem(final UserAgent userAgent) {
        if (userAgent.getPlatform() == ClientPlatform.DESKTOP) {
            if (userAgent.getAdditionalSpecifiers().map(String::toLowerCase).map(ACCEPTABLE_DESKTOP_OS_STRINGS::contains).orElse(false)) {
                meterRegistry.counter(DESKTOP_REQUEST_COUNTER_NAME, OS_TAG, userAgent.getAdditionalSpecifiers().get().toLowerCase()).increment();
            }
        }
    }

    @VisibleForTesting
    void recordAndroidSdkVersion(final UserAgent userAgent) {
        if (userAgent.getPlatform() == ClientPlatform.ANDROID) {
            userAgent.getAdditionalSpecifiers().ifPresent(additionalSpecifiers -> {
                if (additionalSpecifiers.startsWith(ANDROID_SDK_PREFIX)) {
                    try {
                        final int sdkVersion = Integer.parseInt(additionalSpecifiers, ANDROID_SDK_PREFIX.length(), additionalSpecifiers.length(), 10);

                        if (sdkVersion >= MIN_ANDROID_SDK_VERSION && sdkVersion <= MAX_ANDROID_SDK_VERSION) {
                            meterRegistry.counter(ANDROID_REQUEST_COUNTER_NAME, SDK_TAG, String.valueOf(sdkVersion));
                        }
                    } catch (final NumberFormatException ignored) {
                    }
                }
            });
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
