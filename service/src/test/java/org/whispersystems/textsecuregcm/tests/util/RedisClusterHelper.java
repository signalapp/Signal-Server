/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.util.function.Consumer;
import java.util.function.Function;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

public class RedisClusterHelper {

  public static RedisClusterHelper.Builder builder() {
    return new Builder();
  }

  @SuppressWarnings("unchecked")
  private static FaultTolerantRedisCluster buildMockRedisCluster(
      final RedisAdvancedClusterCommands<String, String> stringCommands,
      final RedisAdvancedClusterCommands<byte[], byte[]> binaryCommands,
      final RedisAdvancedClusterAsyncCommands<byte[], byte[]> binaryAsyncCommands,
      final RedisAdvancedClusterReactiveCommands<byte[], byte[]> binaryReactiveCommands) {
    final FaultTolerantRedisCluster cluster = mock(FaultTolerantRedisCluster.class);
    final StatefulRedisClusterConnection<String, String> stringConnection = mock(StatefulRedisClusterConnection.class);
    final StatefulRedisClusterConnection<byte[], byte[]> binaryConnection = mock(StatefulRedisClusterConnection.class);

    when(stringConnection.sync()).thenReturn(stringCommands);
    when(binaryConnection.sync()).thenReturn(binaryCommands);
    when(binaryConnection.async()).thenReturn(binaryAsyncCommands);
    when(binaryConnection.reactive()).thenReturn(binaryReactiveCommands);

    when(cluster.withCluster(any(Function.class))).thenAnswer(invocation -> {
      return invocation.getArgument(0, Function.class).apply(stringConnection);
    });

    doAnswer(invocation -> {
      invocation.getArgument(0, Consumer.class).accept(stringConnection);
      return null;
    }).when(cluster).useCluster(any(Consumer.class));

    when(cluster.withCluster(any(Function.class))).thenAnswer(invocation -> {
      return invocation.getArgument(0, Function.class).apply(stringConnection);
    });

    doAnswer(invocation -> {
      invocation.getArgument(0, Consumer.class).accept(stringConnection);
      return null;
    }).when(cluster).useCluster(any(Consumer.class));

    when(cluster.withBinaryCluster(any(Function.class))).thenAnswer(invocation -> {
      return invocation.getArgument(0, Function.class).apply(binaryConnection);
    });

    doAnswer(invocation -> {
      invocation.getArgument(0, Consumer.class).accept(binaryConnection);
      return null;
    }).when(cluster).useBinaryCluster(any(Consumer.class));

    when(cluster.withBinaryCluster(any(Function.class))).thenAnswer(invocation -> {
      return invocation.getArgument(0, Function.class).apply(binaryConnection);
    });

    doAnswer(invocation -> {
      invocation.getArgument(0, Consumer.class).accept(binaryConnection);
      return null;
    }).when(cluster).useBinaryCluster(any(Consumer.class));

    return cluster;
  }

  @SuppressWarnings("unchecked")
  public static class Builder {

    private RedisAdvancedClusterCommands<String, String> stringCommands = mock(RedisAdvancedClusterCommands.class);
    private RedisAdvancedClusterCommands<byte[], byte[]> binaryCommands = mock(RedisAdvancedClusterCommands.class);
    private RedisAdvancedClusterAsyncCommands<byte[], byte[]> binaryAsyncCommands = mock(
        RedisAdvancedClusterAsyncCommands.class);
    private RedisAdvancedClusterReactiveCommands<byte[], byte[]> binaryReactiveCommands = mock(
        RedisAdvancedClusterReactiveCommands.class);

    private Builder() {

    }

    public Builder stringCommands(final RedisAdvancedClusterCommands<String, String> stringCommands) {
      this.stringCommands = stringCommands;
      return this;
    }

    public Builder binaryCommands(final RedisAdvancedClusterCommands<byte[], byte[]> binaryCommands) {
      this.binaryCommands = binaryCommands;
      return this;
    }

    public Builder binaryAsyncCommands(final RedisAdvancedClusterAsyncCommands<byte[], byte[]> binaryAsyncCommands) {
      this.binaryAsyncCommands = binaryAsyncCommands;
      return this;
    }

    public Builder binaryReactiveCommands(
        final RedisAdvancedClusterReactiveCommands<byte[], byte[]> binaryReactiveCommands) {
      this.binaryReactiveCommands = binaryReactiveCommands;
      return this;
    }

    public FaultTolerantRedisCluster build() {
      return RedisClusterHelper.buildMockRedisCluster(stringCommands, binaryCommands, binaryAsyncCommands,
          binaryReactiveCommands);
    }
  }

}
