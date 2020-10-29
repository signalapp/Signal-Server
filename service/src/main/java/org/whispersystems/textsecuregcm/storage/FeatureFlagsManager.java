/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * The feature flags manager provides a high-throughput, eventually-consistent view of feature flags. This is the main
 * channel through which callers should interact with feature flags.
 * <p/>
 * Feature flags are intended to provide temporary control over server-side features (i.e. for migrations or experiments
 * with new services). Each flag is identified by a human-readable name (e.g. "invert-nano-flappers") and is either
 * active or inactive. Flags (including flags that have not been set) are inactive by default.
 */
public class FeatureFlagsManager implements Managed {

    private final FeatureFlags                          featureFlagDatabase;
    private final ScheduledExecutorService              refreshExecutorService;
    private       ScheduledFuture<?>                    refreshFuture;
    private final AtomicReference<Map<String, Boolean>> featureFlags = new AtomicReference<>(Collections.emptyMap());
    private final Set<Gauge>                            gauges = new HashSet<>();

    private static final String GAUGE_NAME    = "status";
    private static final String FLAG_TAG_NAME = "flag";

    private static final Duration REFRESH_INTERVAL = Duration.ofSeconds(30);

    public FeatureFlagsManager(final FeatureFlags featureFlagDatabase, final ScheduledExecutorService refreshExecutorService) {
        this.featureFlagDatabase    = featureFlagDatabase;
        this.refreshExecutorService = refreshExecutorService;

        refreshFeatureFlags();
    }

    @Override
    public void start() {
        refreshFuture = refreshExecutorService.scheduleAtFixedRate(this::refreshFeatureFlags, 0, REFRESH_INTERVAL.toSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        refreshFuture.cancel(false);
    }

    public boolean isFeatureFlagActive(final String featureFlag) {
        return featureFlags.get().getOrDefault(featureFlag, false);
    }

    public void setFeatureFlag(final String featureFlag, final boolean active) {
        featureFlagDatabase.setFlag(featureFlag, active);
        refreshFeatureFlags();
    }

    public void deleteFeatureFlag(final String featureFlag) {
        featureFlagDatabase.deleteFlag(featureFlag);
        refreshFeatureFlags();
    }

    public Map<String, Boolean> getAllFlags() {
        return featureFlags.get();
    }

    @VisibleForTesting
    void refreshFeatureFlags() {
        final Map<String, Boolean> refreshedFeatureFlags = featureFlagDatabase.getFeatureFlags();

        featureFlags.set(Collections.unmodifiableMap(refreshedFeatureFlags));

        for (final Gauge gauge : gauges) {
            Metrics.globalRegistry.remove(gauge);
        }

        gauges.clear();

        for (final Map.Entry<String, Boolean> entry : refreshedFeatureFlags.entrySet()) {
            final String  featureFlag = entry.getKey();
            final boolean active      = entry.getValue();

            gauges.add(Gauge.builder(name(getClass(), GAUGE_NAME), () -> active ? 1 : 0)
                            .tag(FLAG_TAG_NAME, featureFlag)
                            .register(Metrics.globalRegistry));
        }
    }
}
