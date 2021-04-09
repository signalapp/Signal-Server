/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.ThreadDumpUtil;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.codahale.metrics.MetricRegistry.name;

public class FaultTolerantPubSubConnection<K, V> {

    private final String name;

    private final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection;

    private final CircuitBreaker circuitBreaker;
    private final Retry          retry;

    private final Timer         executeTimer;
    private final Meter         commandTimeoutMeter;
    private final AtomicBoolean wroteThreadDump = new AtomicBoolean(false);

    private static final Logger log = LoggerFactory.getLogger(FaultTolerantPubSubConnection.class);

    public FaultTolerantPubSubConnection(final String name, final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection, final CircuitBreaker circuitBreaker, final Retry retry) {
        this.name             = name;
        this.pubSubConnection = pubSubConnection;
        this.circuitBreaker   = circuitBreaker;
        this.retry            = retry;

        this.pubSubConnection.setNodeMessagePropagation(true);

        final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

        this.executeTimer = metricRegistry.timer(name(getClass(), name + "-pubsub", "execute"));
        this.commandTimeoutMeter = metricRegistry.meter(name(getClass(), name + "-pubsub", "commandTimeout"));
    }

    public void usePubSubConnection(final Consumer<StatefulRedisClusterPubSubConnection<K, V>> consumer) {
        try {
            circuitBreaker.executeCheckedRunnable(() -> retry.executeRunnable(() -> {
                try (final Timer.Context ignored = executeTimer.time()) {
                    consumer.accept(pubSubConnection);
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

    public <T> T withPubSubConnection(final Function<StatefulRedisClusterPubSubConnection<K, V>, T> function) {
        try {
            return circuitBreaker.executeCheckedSupplier(() -> retry.executeCallable(() -> {
                try (final Timer.Context ignored = executeTimer.time()) {
                    return function.apply(pubSubConnection);
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
        log.warn("[{}] Command timeout exception ({}-pubsub)", Thread.currentThread().getName(), this.name, e);

        if (wroteThreadDump.compareAndSet(false, true)) {
            ThreadDumpUtil.writeThreadDump();
        }
    }
}
