package org.whispersystems.textsecuregcm.redis;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisClusterConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisConnectionPoolConfiguration;
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
 * circuit-breaker-protected access to a pool of connections.
 */
public class FaultTolerantRedisCluster {

    private final String name;

    private final RedisClusterClient clusterClient;

    private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> stringConnectionPool;
    private final GenericObjectPool<StatefulRedisClusterConnection<byte[], byte[]>> binaryConnectionPool;

    private final List<StatefulRedisClusterPubSubConnection<?, ?>> pubSubConnections = new ArrayList<>();

    private final CircuitBreakerConfiguration circuitBreakerConfiguration;
    private final CircuitBreaker circuitBreaker;

    private final Timer acquireAndExecuteTimer;
    private final Timer executeTimer;

    private static final Logger log = LoggerFactory.getLogger(FaultTolerantRedisCluster.class);

    public FaultTolerantRedisCluster(final String name, final RedisClusterConfiguration clusterConfiguration) {
        this(name,
             RedisClusterClient.create(clusterConfiguration.getUrls().stream().map(RedisURI::create).collect(Collectors.toList())),
             clusterConfiguration.getTimeout(),
             clusterConfiguration.getCircuitBreakerConfiguration(),
             clusterConfiguration.getConnectionPoolConfiguration());
    }

    @VisibleForTesting
    FaultTolerantRedisCluster(final String name, final RedisClusterClient clusterClient, final Duration timeout, final CircuitBreakerConfiguration circuitBreakerConfiguration, final RedisConnectionPoolConfiguration connectionPoolConfiguration) {
        this.name = name;

        this.clusterClient = clusterClient;
        this.clusterClient.setDefaultTimeout(timeout);

        @SuppressWarnings("rawtypes") final GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxIdle(connectionPoolConfiguration.getPoolSize());
        poolConfig.setMaxTotal(connectionPoolConfiguration.getPoolSize());
        poolConfig.setMaxWaitMillis(connectionPoolConfiguration.getMaxWait().toMillis());

        //noinspection unchecked
        this.stringConnectionPool = ConnectionPoolSupport.createGenericObjectPool(clusterClient::connect, poolConfig);

        //noinspection unchecked
        this.binaryConnectionPool = ConnectionPoolSupport.createGenericObjectPool(() -> clusterClient.connect(ByteArrayCodec.INSTANCE), poolConfig);

        this.circuitBreakerConfiguration = circuitBreakerConfiguration;
        this.circuitBreaker              = CircuitBreaker.of(name + "-read", circuitBreakerConfiguration.toCircuitBreakerConfig());

        CircuitBreakerUtil.registerMetrics(SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME),
                circuitBreaker,
                FaultTolerantRedisCluster.class);

        final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

        this.acquireAndExecuteTimer = metricRegistry.timer(name(getClass(), name, "acquireConnectionAndExecute"));
        this.executeTimer           = metricRegistry.timer(name(getClass(), name, "execute"));
    }

    void shutdown() {
        stringConnectionPool.close();
        binaryConnectionPool.close();

        for (final StatefulRedisClusterPubSubConnection<?, ?> pubSubConnection : pubSubConnections) {
            pubSubConnection.close();
        }

        clusterClient.shutdown();
    }

    public void useCluster(final Consumer<StatefulRedisClusterConnection<String, String>> consumer) {
        acceptPooledConnection(stringConnectionPool, consumer);
    }

    public <T> T withCluster(final Function<StatefulRedisClusterConnection<String, String>, T> function) {
        return applyToPooledConnection(stringConnectionPool, function);
    }

    public void useBinaryCluster(final Consumer<StatefulRedisClusterConnection<byte[], byte[]>> consumer) {
        acceptPooledConnection(binaryConnectionPool, consumer);
    }

    public <T> T withBinaryCluster(final Function<StatefulRedisClusterConnection<byte[], byte[]>, T> function) {
        return applyToPooledConnection(binaryConnectionPool, function);
    }

    private <K, V> void acceptPooledConnection(final GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, final Consumer<StatefulRedisClusterConnection<K, V>> consumer) {
        try {
            circuitBreaker.executeCheckedRunnable(() -> {
                try (final Timer.Context ignored = acquireAndExecuteTimer.time();
                     final StatefulRedisClusterConnection<K, V> connection = pool.borrowObject()) {

                    try (final Timer.Context ignored2 = executeTimer.time()) {
                        consumer.accept(connection);
                    }
                }
            });
        } catch (final Throwable t) {
            log.warn("Redis operation failure", t);

            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else {
                throw new RuntimeException(t);
            }
        }
    }

    private <T, K, V> T applyToPooledConnection(final GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, final Function<StatefulRedisClusterConnection<K, V>, T> function) {
        try {
            return circuitBreaker.executeCheckedSupplier(() -> {
                try (final Timer.Context ignored = acquireAndExecuteTimer.time();
                     final StatefulRedisClusterConnection<K, V> connection = pool.borrowObject()) {

                    try (final Timer.Context ignored2 = executeTimer.time()) {
                        return function.apply(connection);
                    }
                }
            });
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

        return new FaultTolerantPubSubConnection<>(name, pubSubConnection, circuitBreakerConfiguration);
    }
}
