package org.whispersystems.textsecuregcm.tests.util;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RedisServerHelper {

  public static RedisServerHelper.Builder builder() {
    return new RedisServerHelper.Builder();
  }

  @SuppressWarnings("unchecked")
  private static FaultTolerantRedisClient buildMockRedisClient(
      final RedisCommands<String, String> stringCommands,
      final RedisAsyncCommands<String, String> stringAsyncCommands,
      final RedisCommands<byte[], byte[]> binaryCommands,
      final RedisAsyncCommands<byte[], byte[]> binaryAsyncCommands,
      final RedisReactiveCommands<byte[], byte[]> binaryReactiveCommands) {
    final FaultTolerantRedisClient client = mock(FaultTolerantRedisClient.class);
    final StatefulRedisConnection<String, String> stringConnection = mock(StatefulRedisConnection.class);
    final StatefulRedisConnection<byte[], byte[]> binaryConnection = mock(StatefulRedisConnection.class);

    when(stringConnection.sync()).thenReturn(stringCommands);
    when(stringConnection.async()).thenReturn(stringAsyncCommands);
    when(binaryConnection.sync()).thenReturn(binaryCommands);
    when(binaryConnection.async()).thenReturn(binaryAsyncCommands);
    when(binaryConnection.reactive()).thenReturn(binaryReactiveCommands);

    when(client.withConnection(any(Function.class))).thenAnswer(invocation -> {
      return invocation.getArgument(0, Function.class).apply(stringConnection);
    });

    doAnswer(invocation -> {
      invocation.getArgument(0, Consumer.class).accept(stringConnection);
      return null;
    }).when(client).useConnection(any(Consumer.class));

    when(client.withBinaryConnection(any(Function.class))).thenAnswer(invocation -> {
      return invocation.getArgument(0, Function.class).apply(binaryConnection);
    });

    doAnswer(invocation -> {
      invocation.getArgument(0, Consumer.class).accept(binaryConnection);
      return null;
    }).when(client).useBinaryConnection(any(Consumer.class));

    return client;
  }

  @SuppressWarnings("unchecked")
  public static class Builder {

    private RedisCommands<String, String> stringCommands = mock(RedisCommands.class);
    private RedisAsyncCommands<String, String> stringAsyncCommands = mock(RedisAsyncCommands.class);

    private RedisCommands<byte[], byte[]> binaryCommands = mock(RedisCommands.class);

    private RedisAsyncCommands<byte[], byte[]> binaryAsyncCommands =
        mock(RedisAsyncCommands.class);

    private RedisReactiveCommands<byte[], byte[]> binaryReactiveCommands =
        mock(RedisReactiveCommands.class);

    private Builder() {

    }

    public RedisServerHelper.Builder stringCommands(final RedisCommands<String, String> stringCommands) {
      this.stringCommands = stringCommands;
      return this;
    }

    public RedisServerHelper.Builder stringAsyncCommands(final RedisAsyncCommands<String, String> stringAsyncCommands) {
      this.stringAsyncCommands = stringAsyncCommands;
      return this;
    }

    public RedisServerHelper.Builder binaryCommands(final RedisCommands<byte[], byte[]> binaryCommands) {
      this.binaryCommands = binaryCommands;
      return this;
    }

    public RedisServerHelper.Builder binaryAsyncCommands(final RedisAsyncCommands<byte[], byte[]> binaryAsyncCommands) {
      this.binaryAsyncCommands = binaryAsyncCommands;
      return this;
    }

    public RedisServerHelper.Builder binaryReactiveCommands(
        final RedisReactiveCommands<byte[], byte[]> binaryReactiveCommands) {
      this.binaryReactiveCommands = binaryReactiveCommands;
      return this;
    }

    public FaultTolerantRedisClient build() {
      return RedisServerHelper.buildMockRedisClient(stringCommands,
          stringAsyncCommands,
          binaryCommands,
          binaryAsyncCommands,
          binaryReactiveCommands);
    }
  }
}
