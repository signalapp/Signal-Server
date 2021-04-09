/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FaultTolerantRedisClientTest {

    private RedisCommands<String, String> commands;
    private FaultTolerantRedisClient faultTolerantRedisClient;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        final RedisClient redisClient                                   = mock(RedisClient.class);
        final StatefulRedisConnection<String, String> clusterConnection = mock(StatefulRedisConnection.class);

        commands = mock(RedisCommands.class);

        when(redisClient.connect()).thenReturn(clusterConnection);
        when(clusterConnection.sync()).thenReturn(commands);

        final CircuitBreakerConfiguration breakerConfiguration = new CircuitBreakerConfiguration();
        breakerConfiguration.setFailureRateThreshold(100);
        breakerConfiguration.setRingBufferSizeInClosedState(1);
        breakerConfiguration.setWaitDurationInOpenStateInSeconds(Integer.MAX_VALUE);

        faultTolerantRedisClient = new FaultTolerantRedisClient("test", redisClient, Duration.ofSeconds(2), breakerConfiguration);
    }

    @Test
    public void testBreaker() {
        when(commands.get(anyString()))
                     .thenReturn("value")
                     .thenThrow(new io.lettuce.core.RedisException("Badness has ensued."));

        assertEquals("value", faultTolerantRedisClient.withClient(connection -> connection.sync().get("key")));

        assertThrows(RedisException.class,
                () -> faultTolerantRedisClient.withClient(connection -> connection.sync().get("OH NO")));

        assertThrows(CallNotPermittedException.class,
                () -> faultTolerantRedisClient.withClient(connection -> connection.sync().get("OH NO")));
    }
}
