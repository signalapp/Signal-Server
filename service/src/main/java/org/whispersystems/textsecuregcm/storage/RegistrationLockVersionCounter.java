/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import io.dropwizard.metrics.MetricsFactory;
import io.dropwizard.metrics.ReporterFactory;
import io.lettuce.core.KeyValue;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.whispersystems.textsecuregcm.auth.StoredRegistrationLock;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Counts the number of accounts that have the old or new (or neither) versions of a registration lock and publishes
 * the results to our metric aggregator. This class can likely be removed after a few rounds of data collection.
 */
public class RegistrationLockVersionCounter extends AccountDatabaseCrawlerListener {

    private final FaultTolerantRedisCluster redisCluster;
    private final MetricsFactory            metricsFactory;

    static final String REGLOCK_COUNT_KEY = "ReglockVersionCounter::reglockCount";
    static final String PIN_KEY           = "pin";
    static final String REGLOCK_KEY       = "reglock";

    public RegistrationLockVersionCounter(final FaultTolerantRedisCluster redisCluster, final MetricsFactory metricsFactory) {
        this.redisCluster   = redisCluster;
        this.metricsFactory = metricsFactory;
    }

    @Override
    public void onCrawlStart() {
        redisCluster.useCluster(connection -> connection.sync().hset(REGLOCK_COUNT_KEY, Map.of(PIN_KEY, "0", REGLOCK_KEY, "0")));
    }

    @Override
    protected void onCrawlChunk(final Optional<UUID> fromUuid, final List<Account> chunkAccounts) {
        int pinCount     = 0;
        int reglockCount = 0;

        for (final Account account : chunkAccounts) {
            final StoredRegistrationLock storedRegistrationLock = account.getRegistrationLock();

            if (storedRegistrationLock.requiresClientRegistrationLock()) {
                if (storedRegistrationLock.hasDeprecatedPin()) {
                    pinCount++;
                } else {
                    reglockCount++;
                }
            }
        }

        incrementReglockCounts(pinCount, reglockCount);
    }

    private void incrementReglockCounts(final int pinCount, final int reglockCount) {
        redisCluster.useCluster(connection -> {
            final RedisAdvancedClusterCommands<String, String> commands = connection.sync();

            commands.hincrby(REGLOCK_COUNT_KEY, PIN_KEY,     pinCount);
            commands.hincrby(REGLOCK_COUNT_KEY, REGLOCK_KEY, reglockCount);
        });
    }

    @Override
    public void onCrawlEnd(final Optional<UUID> fromUuid) {
        final Map<String, Integer> countsByReglockType =
                redisCluster.withCluster(connection -> connection.sync().hmget(REGLOCK_COUNT_KEY, PIN_KEY, REGLOCK_KEY))
                            .stream()
                            .collect(Collectors.toMap(KeyValue::getKey, keyValue -> keyValue.hasValue() ? keyValue.map(Integer::parseInt).getValue() : 0));

        final MetricRegistry metricRegistry = new MetricRegistry();

        for (final Map.Entry<String, Integer> entry : countsByReglockType.entrySet()) {
            metricRegistry.gauge(name(getClass(), entry.getKey()), () -> entry::getValue);
        }

        for (final ReporterFactory reporterFactory : metricsFactory.getReporters()) {
            try (final ScheduledReporter reporter = reporterFactory.build(metricRegistry)) {
                reporter.report();
            }
        }
    }
}
