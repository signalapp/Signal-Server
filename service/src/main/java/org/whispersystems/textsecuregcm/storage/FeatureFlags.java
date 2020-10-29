/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.jdbi.v3.core.mapper.RowMapper;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Pair;

import java.util.Map;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * The feature flag database is a persistent store of the state of all server-side feature flags. Feature flags are
 * identified by a human-readable name (e.g. "invert-nano-flappers") and are either active or inactive.
 * <p/>
 * The feature flag database provides the most up-to-date possible view of feature flags, but does so at the cost of
 * interacting with a remote data store. In nearly all cases, callers should prefer a cached, eventually-consistent
 * view of feature flags (see {@link FeatureFlagsManager}).
 * <p/>
 * When an operation requiring a feature flag has finished, callers should delete the feature flag to prevent
 * accumulation of non-functional flags.
 */
public class FeatureFlags {

    private final FaultTolerantDatabase database;

    private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
    private final Timer getAllTimer             = metricRegistry.timer(name(getClass(), "getAll"));
    private final Timer updateTimer             = metricRegistry.timer(name(getClass(), "update"));
    private final Timer deleteTimer             = metricRegistry.timer(name(getClass(), "delete"));
    private final Timer vacuumTimer             = metricRegistry.timer(name(getClass(), "vacuum"));

    private static final RowMapper<Pair<String, Boolean>> PAIR_ROW_MAPPER = (resultSet, statementContext) ->
            new Pair<>(resultSet.getString("flag"), resultSet.getBoolean("active"));

    public FeatureFlags(final FaultTolerantDatabase database) {
        this.database = database;
    }

    public Map<String, Boolean> getFeatureFlags() {
        try (final Timer.Context ignored = getAllTimer.time()) {
            return database.with(jdbi -> jdbi.withHandle(handle -> handle.createQuery("SELECT flag, active FROM feature_flags")
                                             .map(PAIR_ROW_MAPPER)
                                             .list()
                                             .stream()
                                             .collect(Collectors.toMap(Pair::first, Pair::second))));
        }
    }

    public void setFlag(final String featureFlag, final boolean active) {
        try (final Timer.Context ignored = updateTimer.time()) {
            database.use(jdbi -> jdbi.withHandle(handle -> handle.createUpdate("INSERT INTO feature_flags (flag, active) VALUES (:featureFlag, :active) ON CONFLICT (flag) DO UPDATE SET active = EXCLUDED.active")
                                     .bind("featureFlag", featureFlag)
                                     .bind("active", active)
                                     .execute()));
        }
    }

    public void deleteFlag(final String featureFlag) {
        try (final Timer.Context ignored = deleteTimer.time()) {
            database.use(jdbi -> jdbi.withHandle(handle -> handle.createUpdate("DELETE FROM feature_flags WHERE flag = :featureFlag")
                    .bind("featureFlag", featureFlag)
                    .execute()));
        }
    }

    public void vacuum() {
        try (final Timer.Context ignored = vacuumTimer.time()) {
            database.use(jdbi -> jdbi.useHandle(handle -> {
                handle.execute("VACUUM feature_flags");
            }));
        }
    }
}
