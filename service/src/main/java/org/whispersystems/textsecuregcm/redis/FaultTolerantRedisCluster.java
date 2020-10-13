package org.whispersystems.textsecuregcm.redis;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.event.connection.ConnectionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisClusterConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.util.Constants;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * A fault-tolerant access manager for a Redis cluster. A fault-tolerant Redis cluster provides managed,
 * circuit-breaker-protected access to connections.
 */
public class FaultTolerantRedisCluster {

    private final String name;

    private final RedisClusterClient clusterClient;

    private final StatefulRedisClusterConnection<String, String> stringConnection;
    private final StatefulRedisClusterConnection<byte[], byte[]> binaryConnection;

    private final List<StatefulRedisClusterPubSubConnection<?, ?>> pubSubConnections = new ArrayList<>();

    private final CircuitBreaker circuitBreaker;
    private final Retry          retry;

    private final Timer executeTimer;

    private static final Logger log = LoggerFactory.getLogger(FaultTolerantRedisCluster.class);

    public FaultTolerantRedisCluster(final String name, final RedisClusterConfiguration clusterConfiguration) {
        this(name,
             RedisClusterClient.create(clusterConfiguration.getUrls().stream().map(RedisURI::create).collect(Collectors.toList())),
             clusterConfiguration.getTimeout(),
             clusterConfiguration.getCircuitBreakerConfiguration(),
             clusterConfiguration.getRetryConfiguration());
    }

    @VisibleForTesting
    FaultTolerantRedisCluster(final String name, final RedisClusterClient clusterClient, final Duration commandTimeout, final CircuitBreakerConfiguration circuitBreakerConfiguration, final RetryConfiguration retryConfiguration) {
        this.name = name;

        this.clusterClient = clusterClient;
        this.clusterClient.setDefaultTimeout(commandTimeout);

        this.clusterClient.getResources().eventBus().get().subscribe(event -> {
            if (event instanceof ConnectionEvent) {
                log.info("Connection event for {}: {}", this.name, event);
            } else if (event instanceof ClusterTopologyChangedEvent) {
                log.info("Cluster topology for {} changed: {}", this.name, event);
            }
        });

        this.stringConnection = clusterClient.connect();
        this.binaryConnection = clusterClient.connect(ByteArrayCodec.INSTANCE);

        this.circuitBreaker = CircuitBreaker.of(name + "-breaker", circuitBreakerConfiguration.toCircuitBreakerConfig());
        this.retry          = Retry.of(name + "-retry", retryConfiguration.toRetryConfigBuilder().retryOnException(exception -> exception instanceof RedisCommandTimeoutException).build());

        CircuitBreakerUtil.registerMetrics(SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME), circuitBreaker, FaultTolerantRedisCluster.class);
        CircuitBreakerUtil.registerMetrics(SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME), retry, FaultTolerantRedisCluster.class);

        final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

        this.executeTimer = metricRegistry.timer(name(getClass(), name, "execute"));
    }

    void shutdown() {
        stringConnection.close();
        binaryConnection.close();

        for (final StatefulRedisClusterPubSubConnection<?, ?> pubSubConnection : pubSubConnections) {
            pubSubConnection.close();
        }

        clusterClient.shutdown();
    }

    public void useCluster(final Consumer<StatefulRedisClusterConnection<String, String>> consumer) {
        useConnection(stringConnection, consumer);
    }

    public <T> T withCluster(final Function<StatefulRedisClusterConnection<String, String>, T> function) {
        return withConnection(stringConnection, function);
    }

    public void useBinaryCluster(final Consumer<StatefulRedisClusterConnection<byte[], byte[]>> consumer) {
        useConnection(binaryConnection, consumer);
    }

    public <T> T withBinaryCluster(final Function<StatefulRedisClusterConnection<byte[], byte[]>, T> function) {
        return withConnection(binaryConnection, function);
    }

    private <K, V> void useConnection(final StatefulRedisClusterConnection<K, V> connection, final Consumer<StatefulRedisClusterConnection<K, V>> consumer) {
        try {
            circuitBreaker.executeCheckedRunnable(() -> retry.executeRunnable(() -> {
                try (final Timer.Context ignored = executeTimer.time()) {
                    consumer.accept(connection);
                }
            }));
        } catch (final Throwable t) {
            log.warn("Redis operation failure", t);

            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else {
                throw new RuntimeException(t);
            }
        }
    }

    private <T, K, V> T withConnection(final StatefulRedisClusterConnection<K, V> connection, final Function<StatefulRedisClusterConnection<K, V>, T> function) {
        try {
            return circuitBreaker.executeCheckedSupplier(() -> retry.executeCallable(() -> {
                try (final Timer.Context ignored = executeTimer.time()) {
                    return function.apply(connection);
                }
            }));
        } catch (final Throwable t) {
            log.warn("Redis operation failure", t);

            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else {
                throw new RuntimeException(t);
            }
        }
    }

    public FaultTolerantPubSubConnection<String, String> createPubSubConnection() {
        final StatefulRedisClusterPubSubConnection<String, String> pubSubConnection = clusterClient.connectPubSub();
        pubSubConnections.add(pubSubConnection);

        return new FaultTolerantPubSubConnection<>(name, pubSubConnection, circuitBreaker, retry);
    }
}
