package org.whispersystems.textsecuregcm.redis;

import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.util.Constants;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A fault-tolerant access manager for a Redis cluster. A fault-tolerant Redis cluster has separate circuit breakers for
 * read and write operations because the leader in a Redis cluster shard may fail while its read-only replicas can still
 * serve traffic.
 */
public class FaultTolerantRedisCluster {

    private final String name;

    private final RedisClusterClient clusterClient;

    private final StatefulRedisClusterConnection<String, String> stringClusterConnection;
    private final StatefulRedisClusterConnection<byte[], byte[]> binaryClusterConnection;

    private final List<StatefulRedisClusterPubSubConnection<?, ?>> pubSubConnections = new ArrayList<>();

    private final CircuitBreakerConfiguration circuitBreakerConfiguration;
    private final CircuitBreaker circuitBreaker;

    public FaultTolerantRedisCluster(final String name, final List<String> urls, final Duration timeout, final CircuitBreakerConfiguration circuitBreakerConfiguration) {
        this(name, RedisClusterClient.create(urls.stream().map(RedisURI::create).collect(Collectors.toList())), timeout, circuitBreakerConfiguration);
    }

    @VisibleForTesting
    FaultTolerantRedisCluster(final String name, final RedisClusterClient clusterClient, final Duration timeout, final CircuitBreakerConfiguration circuitBreakerConfiguration) {
        this.name = name;

        this.clusterClient = clusterClient;
        this.clusterClient.setDefaultTimeout(timeout);

        this.stringClusterConnection = clusterClient.connect();
        this.binaryClusterConnection = clusterClient.connect(ByteArrayCodec.INSTANCE);

        this.circuitBreakerConfiguration = circuitBreakerConfiguration;
        this.circuitBreaker              = CircuitBreaker.of(name + "-read", circuitBreakerConfiguration.toCircuitBreakerConfig());

        CircuitBreakerUtil.registerMetrics(SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME),
                circuitBreaker,
                FaultTolerantRedisCluster.class);
    }

    void shutdown() {
        stringClusterConnection.close();
        binaryClusterConnection.close();

        for (final StatefulRedisClusterPubSubConnection<?, ?> pubSubConnection : pubSubConnections) {
            pubSubConnection.close();
        }

        clusterClient.shutdown();
    }

    public void useCluster(final Consumer<StatefulRedisClusterConnection<String, String>> consumer) {
        this.circuitBreaker.executeRunnable(() -> consumer.accept(stringClusterConnection));
    }

    public <T> T withCluster(final Function<StatefulRedisClusterConnection<String, String>, T> consumer) {
        return this.circuitBreaker.executeSupplier(() -> consumer.apply(stringClusterConnection));
    }

    public void useBinaryCluster(final Consumer<StatefulRedisClusterConnection<byte[], byte[]>> consumer) {
        this.circuitBreaker.executeRunnable(() -> consumer.accept(binaryClusterConnection));
    }

    public <T> T withBinaryCluster(final Function<StatefulRedisClusterConnection<byte[], byte[]>, T> consumer) {
        return this.circuitBreaker.executeSupplier(() -> consumer.apply(binaryClusterConnection));
    }

    public FaultTolerantPubSubConnection<String, String> createPubSubConnection() {
        final StatefulRedisClusterPubSubConnection<String, String> pubSubConnection = clusterClient.connectPubSub();
        pubSubConnections.add(pubSubConnection);

        return new FaultTolerantPubSubConnection<>(name, pubSubConnection, circuitBreakerConfiguration);
    }
}
