package org.whispersystems.textsecuregcm.redis;

import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A fault-tolerant access manager for a Redis cluster. A fault-tolerant Redis cluster has separate circuit breakers for
 * read and write operations because the leader in a Redis cluster shard may fail while its read-only replicas can still
 * serve traffic.
 */
public class FaultTolerantRedisCluster {

    private final StatefulRedisClusterConnection<String, String> clusterConnection;

    private final CircuitBreaker readCircuitBreaker;
    private final CircuitBreaker writeCircuitBreaker;

    public FaultTolerantRedisCluster(final String name, final RedisClusterClient clusterClient, final CircuitBreakerConfiguration circuitBreakerConfiguration) {
        this.clusterConnection   = clusterClient.connect();
        this.readCircuitBreaker  = CircuitBreaker.of(name + "-read", circuitBreakerConfiguration.toCircuitBreakerConfig());
        this.writeCircuitBreaker = CircuitBreaker.of(name + "-write", circuitBreakerConfiguration.toCircuitBreakerConfig());

        CircuitBreakerUtil.registerMetrics(SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME),
                readCircuitBreaker,
                FaultTolerantRedisCluster.class);

        CircuitBreakerUtil.registerMetrics(SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME),
                writeCircuitBreaker,
                FaultTolerantRedisCluster.class);
    }

    public void useReadCluster(Consumer<StatefulRedisClusterConnection<String, String>> consumer) {
        this.readCircuitBreaker.executeRunnable(() -> consumer.accept(clusterConnection));
    }

    public <T> T withReadCluster(Function<StatefulRedisClusterConnection<String, String>, T> consumer) {
        return this.readCircuitBreaker.executeSupplier(() -> consumer.apply(clusterConnection));
    }

    public void useWriteCluster(Consumer<StatefulRedisClusterConnection<String, String>> consumer) {
        this.writeCircuitBreaker.executeRunnable(() -> consumer.accept(clusterConnection));
    }

    public <T> T withWriteCluster(Function<StatefulRedisClusterConnection<String, String>, T> consumer) {
        return this.writeCircuitBreaker.executeSupplier(() -> consumer.apply(clusterConnection));
    }
}
