/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisConfiguration;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.util.Constants;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.codahale.metrics.MetricRegistry.name;

public class FaultTolerantRedisClient {

    private final RedisClient client;

    private final StatefulRedisConnection<String, String> stringConnection;
    private final StatefulRedisConnection<byte[], byte[]> binaryConnection;
    private final CircuitBreaker                          circuitBreaker;

    private final Timer executeTimer;

    private static final Logger log = LoggerFactory.getLogger(FaultTolerantRedisClient.class);

    public FaultTolerantRedisClient(final String name, final RedisConfiguration redisConfiguration) {
        this(name, RedisClient.create(redisConfiguration.getUrl()), redisConfiguration.getTimeout(), redisConfiguration.getCircuitBreakerConfiguration());
    }

    @VisibleForTesting
    FaultTolerantRedisClient(final String name, final RedisClient redisClient, final Duration commandTimeout, final CircuitBreakerConfiguration circuitBreakerConfiguration) {
        this.client = redisClient;
        this.client.setDefaultTimeout(commandTimeout);

        this.stringConnection = client.connect();
        this.binaryConnection = client.connect(ByteArrayCodec.INSTANCE);

        this.circuitBreaker = CircuitBreaker.of(name + "-breaker", circuitBreakerConfiguration.toCircuitBreakerConfig());

        CircuitBreakerUtil.registerMetrics(SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME),
                circuitBreaker,
                FaultTolerantRedisCluster.class);

        this.executeTimer = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME).timer(name(getClass(), name, "execute"));
    }

    @VisibleForTesting
    void shutdown() {
        stringConnection.close();
        client.shutdown();
    }

    public void useClient(final Consumer<StatefulRedisConnection<String, String>> consumer) {
        useConnection(stringConnection, consumer);
    }

    public <T> T withClient(final Function<StatefulRedisConnection<String, String>, T> function) {
        return withConnection(stringConnection, function);
    }

    public void useBinaryClient(final Consumer<StatefulRedisConnection<byte[], byte[]>> consumer) {
        useConnection(binaryConnection, consumer);
    }

    public <T> T withBinaryClient(final Function<StatefulRedisConnection<byte[], byte[]>, T> function) {
        return withConnection(binaryConnection, function);
    }

    private <K, V> void useConnection(final StatefulRedisConnection<K, V> connection, final Consumer<StatefulRedisConnection<K, V>> consumer) {
        try {
            circuitBreaker.executeCheckedRunnable(() -> {
                try (final Timer.Context ignored = executeTimer.time()) {
                    consumer.accept(connection);
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

    private <T, K, V> T withConnection(final StatefulRedisConnection<K, V> connection, final Function<StatefulRedisConnection<K, V>, T> function) {
        try {
            return circuitBreaker.executeCheckedSupplier(() -> {
                try (final Timer.Context ignored = executeTimer.time()) {
                    return function.apply(connection);
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
}
