/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.pubsub.api.sync.RedisClusterPubSubCommands;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;

class FaultTolerantPubSubConnectionTest {

    private RedisClusterPubSubCommands<String, String>    pubSubCommands;
    private FaultTolerantPubSubConnection<String, String> faultTolerantPubSubConnection;

    @SuppressWarnings("unchecked")
    @BeforeEach
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
    void testBreaker() {
        when(pubSubCommands.get(anyString()))
                .thenReturn("value")
                .thenThrow(new RuntimeException("Badness has ensued."));

        assertEquals("value", faultTolerantPubSubConnection.withPubSubConnection(connection -> connection.sync().get("key")));

        assertThrows(RedisException.class,
                () -> faultTolerantPubSubConnection.withPubSubConnection(connection -> connection.sync().get("OH NO")));

        final RedisException redisException = assertThrows(RedisException.class,
                () -> faultTolerantPubSubConnection.withPubSubConnection(connection -> connection.sync().get("OH NO")));

        assertTrue(redisException.getCause() instanceof CallNotPermittedException);
    }

    @Test
    void testRetry() {
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
