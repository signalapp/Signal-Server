package org.whispersystems.textsecuregcm.redis;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisConnectionPoolConfiguration;

import java.time.Duration;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FaultTolerantRedisClusterTest {

    private RedisAdvancedClusterCommands<String, String> clusterCommands;
    private FaultTolerantRedisCluster                    faultTolerantCluster;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        final RedisClusterClient                                   clusterClient     = mock(RedisClusterClient.class);
        final StatefulRedisClusterConnection<String, String>       clusterConnection = mock(StatefulRedisClusterConnection.class);
        final StatefulRedisClusterPubSubConnection<String, String> pubSubConnection  = mock(StatefulRedisClusterPubSubConnection.class);

        clusterCommands = mock(RedisAdvancedClusterCommands.class);

        when(clusterClient.connect()).thenReturn(clusterConnection);
        when(clusterClient.connectPubSub()).thenReturn(pubSubConnection);
        when(clusterConnection.sync()).thenReturn(clusterCommands);

        final CircuitBreakerConfiguration breakerConfiguration = new CircuitBreakerConfiguration();
        breakerConfiguration.setFailureRateThreshold(100);
        breakerConfiguration.setRingBufferSizeInClosedState(1);
        breakerConfiguration.setWaitDurationInOpenStateInSeconds(Integer.MAX_VALUE);

        faultTolerantCluster = new FaultTolerantRedisCluster("test", clusterClient, Duration.ofSeconds(2), breakerConfiguration, new RedisConnectionPoolConfiguration());
    }

    @Test
    public void testBreaker() {
        when(clusterCommands.get(anyString()))
                .thenReturn("value")
                .thenThrow(new RedisException("Badness has ensued."));

        assertEquals("value", faultTolerantCluster.withCluster(connection -> connection.sync().get("key")));

        assertThrows(RedisException.class,
                () -> faultTolerantCluster.withCluster(connection -> connection.sync().get("OH NO")));

        assertThrows(CallNotPermittedException.class,
                () -> faultTolerantCluster.withCluster(connection -> connection.sync().get("OH NO")));
    }
}
