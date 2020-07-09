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
    public static FaultTolerantRedisCluster buildMockRedisCluster(final RedisAdvancedClusterCommands<String, String> stringCommands) {
        return buildMockRedisCluster(stringCommands, mock(RedisAdvancedClusterCommands.class));
    }

    @SuppressWarnings("unchecked")
    public static FaultTolerantRedisCluster buildMockRedisCluster(final RedisAdvancedClusterCommands<String, String> stringCommands, final RedisAdvancedClusterCommands<byte[], byte[]> binaryCommands) {
        final FaultTolerantRedisCluster                      cluster    = mock(FaultTolerantRedisCluster.class);
        final StatefulRedisClusterConnection<String, String> stringConnection = mock(StatefulRedisClusterConnection.class);
        final StatefulRedisClusterConnection<byte[], byte[]> binaryConnection = mock(StatefulRedisClusterConnection.class);

        when(stringConnection.sync()).thenReturn(stringCommands);
        when(binaryConnection.sync()).thenReturn(binaryCommands);

        when(cluster.withReadCluster(any(Function.class))).thenAnswer(invocation -> {
            return invocation.getArgument(0, Function.class).apply(stringConnection);
        });
        
        doAnswer(invocation -> {
            invocation.getArgument(0, Consumer.class).accept(stringConnection);
            return null;
        }).when(cluster).useReadCluster(any(Consumer.class));

        when(cluster.withWriteCluster(any(Function.class))).thenAnswer(invocation -> {
            return invocation.getArgument(0, Function.class).apply(stringConnection);
        });

        doAnswer(invocation -> {
            invocation.getArgument(0, Consumer.class).accept(stringConnection);
            return null;
        }).when(cluster).useWriteCluster(any(Consumer.class));

        when(cluster.withBinaryReadCluster(any(Function.class))).thenAnswer(invocation -> {
            return invocation.getArgument(0, Function.class).apply(binaryConnection);
        });

        doAnswer(invocation -> {
            invocation.getArgument(0, Consumer.class).accept(binaryConnection);
            return null;
        }).when(cluster).useBinaryReadCluster(any(Consumer.class));

        when(cluster.withBinaryWriteCluster(any(Function.class))).thenAnswer(invocation -> {
            return invocation.getArgument(0, Function.class).apply(binaryConnection);
        });

        doAnswer(invocation -> {
            invocation.getArgument(0, Consumer.class).accept(binaryConnection);
            return null;
        }).when(cluster).useBinaryWriteCluster(any(Consumer.class));
        
        return cluster;
    }
}
