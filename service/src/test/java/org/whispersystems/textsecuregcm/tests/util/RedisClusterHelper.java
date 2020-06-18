package org.whispersystems.textsecuregcm.tests.util;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

import java.util.function.Consumer;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RedisClusterHelper {

    @SuppressWarnings("unchecked")
    public static FaultTolerantRedisCluster buildMockRedisCluster(final RedisAdvancedClusterCommands<String, String> commands) {
        final FaultTolerantRedisCluster                      cluster    = mock(FaultTolerantRedisCluster.class);
        final StatefulRedisClusterConnection<String, String> connection = mock(StatefulRedisClusterConnection.class);

        when(connection.sync()).thenReturn(commands);

        when(cluster.withReadCluster(any(Function.class))).thenAnswer(invocation -> {
            return invocation.getArgument(0, Function.class).apply(connection);
        });
        
        doAnswer(invocation -> {
            invocation.getArgument(0, Consumer.class).accept(connection);
            return null;
        }).when(cluster).useReadCluster(any(Consumer.class));

        when(cluster.withWriteCluster(any(Function.class))).thenAnswer(invocation -> {
            return invocation.getArgument(0, Function.class).apply(connection);
        });

        doAnswer(invocation -> {
            invocation.getArgument(0, Consumer.class).accept(connection);
            return null;
        }).when(cluster).useWriteCluster(any(Consumer.class));
        
        return cluster;
    }
}
