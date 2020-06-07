package org.whispersystems.textsecuregcm.redis;

import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.util.Constants;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A fault-tolerant access manager for a Redis cluster. A fault-tolerant Redis cluster has separate circuit breakers for
 * read and write operations because the leader in a Redis cluster shard may fail while its read-only replicas can still
 * serve traffic.
 */
public class FaultTolerantRedisCluster implements Managed {

    private final RedisClusterClient clusterClient;
    private final StatefulRedisClusterConnection<String, String> clusterConnection;

    private final CircuitBreaker readCircuitBreaker;
    private final CircuitBreaker writeCircuitBreaker;

    public FaultTolerantRedisCluster(final String name, final List<String> urls, final Duration timeout, final CircuitBreakerConfiguration circuitBreakerConfiguration) {
        this(name, RedisClusterClient.create(urls.stream().map(RedisURI::create).collect(Collectors.toList())), timeout, circuitBreakerConfiguration);
    }

    @VisibleForTesting
    FaultTolerantRedisCluster(final String name, final RedisClusterClient clusterClient, final Duration timeout, final CircuitBreakerConfiguration circuitBreakerConfiguration) {
        this.clusterClient = clusterClient;
        this.clusterClient.setDefaultTimeout(timeout);

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

    public void useReadCluster(final Consumer<StatefulRedisClusterConnection<String, String>> consumer) {
        this.readCircuitBreaker.executeRunnable(() -> consumer.accept(clusterConnection));
    }

    public <T> T withReadCluster(final Function<StatefulRedisClusterConnection<String, String>, T> consumer) {
        return this.readCircuitBreaker.executeSupplier(() -> consumer.apply(clusterConnection));
    }

    public void useWriteCluster(final Consumer<StatefulRedisClusterConnection<String, String>> consumer) {
        this.writeCircuitBreaker.executeRunnable(() -> consumer.accept(clusterConnection));
    }

    public <T> T withWriteCluster(final Function<StatefulRedisClusterConnection<String, String>, T> consumer) {
        return this.writeCircuitBreaker.executeSupplier(() -> consumer.apply(clusterConnection));
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        clusterClient.shutdown();
    }
}
