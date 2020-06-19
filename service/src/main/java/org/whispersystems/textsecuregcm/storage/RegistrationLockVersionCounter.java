package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.MetricRegistry;
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
    static final String NO_REGLOCK_KEY    = "none";
    static final String PIN_ONLY_KEY      = "pinOnly";
    static final String REGLOCK_ONLY_KEY  = "reglockOnly";
    static final String BOTH_KEY          = "both";

    public RegistrationLockVersionCounter(final FaultTolerantRedisCluster redisCluster, final MetricsFactory metricsFactory) {
        this.redisCluster   = redisCluster;
        this.metricsFactory = metricsFactory;
    }

    @Override
    public void onCrawlStart() {
        redisCluster.useWriteCluster(connection -> connection.sync().hset(REGLOCK_COUNT_KEY, Map.of(
                     NO_REGLOCK_KEY,   "0",
                     PIN_ONLY_KEY,     "0",
                     REGLOCK_ONLY_KEY, "0",
                     BOTH_KEY,         "0")));
    }

    @Override
    protected void onCrawlChunk(final Optional<UUID> fromUuid, final List<Account> chunkAccounts) {
        int noReglockCount   = 0;
        int pinOnlyCount     = 0;
        int reglockOnlyCount = 0;
        int bothCount        = 0;

        for (final Account account : chunkAccounts) {
            final StoredRegistrationLock storedRegistrationLock = account.getRegistrationLock();

            if (storedRegistrationLock.hasDeprecatedPin() && storedRegistrationLock.needsFailureCredentials()) {
                bothCount++;
            } else if (storedRegistrationLock.hasDeprecatedPin()) {
                pinOnlyCount++;
            } else if (storedRegistrationLock.needsFailureCredentials()) {
                reglockOnlyCount++;
            } else {
                noReglockCount++;
            }
        }

        incrementReglockCounts(noReglockCount, pinOnlyCount, reglockOnlyCount, bothCount);
    }

    private void incrementReglockCounts(final int noReglockCount, final int pinOnlyCount, final int reglockOnlyCount, final int bothCount) {
        redisCluster.useWriteCluster(connection -> {
            final RedisAdvancedClusterCommands<String, String> commands = connection.sync();

            commands.hincrby(REGLOCK_COUNT_KEY, NO_REGLOCK_KEY,   noReglockCount);
            commands.hincrby(REGLOCK_COUNT_KEY, PIN_ONLY_KEY,     pinOnlyCount);
            commands.hincrby(REGLOCK_COUNT_KEY, REGLOCK_ONLY_KEY, reglockOnlyCount);
            commands.hincrby(REGLOCK_COUNT_KEY, BOTH_KEY,         bothCount);
        });
    }

    @Override
    public void onCrawlEnd(final Optional<UUID> fromUuid) {
        final Map<String, Integer> countsByReglockType =
                redisCluster.withReadCluster(connection -> connection.sync().hmget(REGLOCK_COUNT_KEY, NO_REGLOCK_KEY, PIN_ONLY_KEY, REGLOCK_ONLY_KEY, BOTH_KEY))
                            .stream()
                            .collect(Collectors.toMap(KeyValue::getKey, keyValue -> keyValue.hasValue() ? keyValue.map(Integer::parseInt).getValue() : 0));

        final MetricRegistry metricRegistry = new MetricRegistry();

        for (final Map.Entry<String, Integer> entry : countsByReglockType.entrySet()) {
            metricRegistry.gauge(name(getClass(), entry.getKey()), () -> entry::getValue);
        }

        for (final ReporterFactory reporterFactory : metricsFactory.getReporters()) {
            reporterFactory.build(metricRegistry).report();
        }
    }
}
