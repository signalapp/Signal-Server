/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisClusterConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.ThreadDumpUtil;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private final Meter         commandTimeoutMeter;
    private final AtomicBoolean wroteThreadDump = new AtomicBoolean(false);

    private static final Logger log = LoggerFactory.getLogger(FaultTolerantRedisCluster.class);

    public FaultTolerantRedisCluster(final String name, final RedisClusterConfiguration clusterConfiguration, final ClientResources clientResources) {
        this(name,
             RedisClusterClient.create(clientResources, clusterConfiguration.getUrls().stream().map(RedisURI::create).collect(Collectors.toList())),
             clusterConfiguration.getTimeout(),
             clusterConfiguration.getCircuitBreakerConfiguration(),
             clusterConfiguration.getRetryConfiguration());
    }

    @VisibleForTesting
    FaultTolerantRedisCluster(final String name, final RedisClusterClient clusterClient, final Duration commandTimeout, final CircuitBreakerConfiguration circuitBreakerConfiguration, final RetryConfiguration retryConfiguration) {
        this.name = name;

        final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
        this.commandTimeoutMeter = metricRegistry.meter(name(getClass(), this.name, "commandTimeout"));

        this.clusterClient = clusterClient;
        this.clusterClient.setDefaultTimeout(commandTimeout);
        this.clusterClient.setOptions(ClusterClientOptions.builder()
                                                          .validateClusterNodeMembership(false)
                                                          .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
                                                                                                               .enablePeriodicRefresh()
                                                                                                               .enableAllAdaptiveRefreshTriggers()
                                                                                                               .build())
                                                          .build());

        this.stringConnection = clusterClient.connect();
        this.binaryConnection = clusterClient.connect(ByteArrayCodec.INSTANCE);

        this.circuitBreaker = CircuitBreaker.of(name + "-breaker", circuitBreakerConfiguration.toCircuitBreakerConfig());
        this.retry          = Retry.of(name + "-retry", retryConfiguration.toRetryConfigBuilder().retryOnException(exception -> exception instanceof RedisCommandTimeoutException).build());

        CircuitBreakerUtil.registerMetrics(SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME), circuitBreaker, FaultTolerantRedisCluster.class);
        CircuitBreakerUtil.registerMetrics(SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME), retry, FaultTolerantRedisCluster.class);
    }

    void shutdown() {
        stringConnection.close();
        binaryConnection.close();

        for (final StatefulRedisClusterPubSubConnection<?, ?> pubSubConnection : pubSubConnections) {
            pubSubConnection.close();
        }

        clusterClient.shutdown();
    }

    public String getName() {
        return name;
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
                try {
                    consumer.accept(connection);
                } catch (final RedisCommandTimeoutException e) {
                    recordCommandTimeout(e);
                    throw e;
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
                try {
                    return function.apply(connection);
                } catch (final RedisCommandTimeoutException e) {
                    recordCommandTimeout(e);
                    throw e;
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

    private void recordCommandTimeout(final RedisCommandTimeoutException e) {
        commandTimeoutMeter.mark();
        log.warn("[{}] Command timeout exception ({})", Thread.currentThread().getName(), this.name, e);

        if (wroteThreadDump.compareAndSet(false, true)) {
            ThreadDumpUtil.writeThreadDump();
        }
    }

    public FaultTolerantPubSubConnection<String, String> createPubSubConnection() {
        final StatefulRedisClusterPubSubConnection<String, String> pubSubConnection = clusterClient.connectPubSub();
        pubSubConnections.add(pubSubConnection);

        return new FaultTolerantPubSubConnection<>(name, pubSubConnection, circuitBreaker, retry);
    }
}
