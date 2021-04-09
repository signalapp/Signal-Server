/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.pubsub.api.sync.RedisClusterPubSubCommands;
import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FaultTolerantPubSubConnectionTest {

    private RedisClusterPubSubCommands<String, String>    pubSubCommands;
    private FaultTolerantPubSubConnection<String, String> faultTolerantPubSubConnection;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        final StatefulRedisClusterPubSubConnection<String, String> pubSubConnection = mock(StatefulRedisClusterPubSubConnection.class);

        pubSubCommands = mock(RedisClusterPubSubCommands.class);

        when(pubSubConnection.sync()).thenReturn(pubSubCommands);

        final CircuitBreakerConfiguration breakerConfiguration = new CircuitBreakerConfiguration();
        breakerConfiguration.setFailureRateThreshold(100);
        breakerConfiguration.setRingBufferSizeInClosedState(1);
        breakerConfiguration.setWaitDurationInOpenStateInSeconds(Integer.MAX_VALUE);

        final RetryConfiguration retryConfiguration = new RetryConfiguration();
        retryConfiguration.setMaxAttempts(3);
        retryConfiguration.setWaitDuration(0);

        final CircuitBreaker circuitBreaker = CircuitBreaker.of("test", breakerConfiguration.toCircuitBreakerConfig());
        final Retry          retry          = Retry.of("test", retryConfiguration.toRetryConfig());

        faultTolerantPubSubConnection = new FaultTolerantPubSubConnection<>("test", pubSubConnection, circuitBreaker, retry);
    }

    @Test
    public void testBreaker() {
        when(pubSubCommands.get(anyString()))
                .thenReturn("value")
                .thenThrow(new io.lettuce.core.RedisException("Badness has ensued."));

        assertEquals("value", faultTolerantPubSubConnection.withPubSubConnection(connection -> connection.sync().get("key")));

        assertThrows(RedisException.class,
                () -> faultTolerantPubSubConnection.withPubSubConnection(connection -> connection.sync().get("OH NO")));

        assertThrows(CallNotPermittedException.class,
                () -> faultTolerantPubSubConnection.withPubSubConnection(connection -> connection.sync().get("OH NO")));
    }

    @Test
    public void testRetry() {
        when(pubSubCommands.get(anyString()))
                .thenThrow(new RedisCommandTimeoutException())
                .thenThrow(new RedisCommandTimeoutException())
                .thenReturn("value");

        assertEquals("value", faultTolerantPubSubConnection.withPubSubConnection(connection -> connection.sync().get("key")));

        when(pubSubCommands.get(anyString()))
                .thenThrow(new RedisCommandTimeoutException())
                .thenThrow(new RedisCommandTimeoutException())
                .thenThrow(new RedisCommandTimeoutException())
                .thenReturn("value");

        assertThrows(RedisCommandTimeoutException.class, () -> faultTolerantPubSubConnection.withPubSubConnection(connection -> connection.sync().get("key")));
    }
}
