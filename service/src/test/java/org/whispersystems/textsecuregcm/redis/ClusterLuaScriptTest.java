/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lettuce.core.FlushMode;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.RedisCommand;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import reactor.core.publisher.Flux;

class ClusterLuaScriptTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @Test
  void testExecute() {
    final RedisAdvancedClusterCommands<String, String> commands = mock(RedisAdvancedClusterCommands.class);
    final FaultTolerantRedisClusterClient mockCluster = RedisClusterHelper.builder().stringCommands(commands).build();

    final String script = "return redis.call(\"SET\", KEYS[1], ARGV[1])";
    final ScriptOutputType scriptOutputType = ScriptOutputType.VALUE;
    final List<String> keys = List.of("key");
    final List<String> values = List.of("value");

    when(commands.evalsha(any(), any(), any(), any())).thenReturn("OK");

    final ClusterLuaScript luaScript = new ClusterLuaScript(mockCluster, script, scriptOutputType);
    luaScript.execute(keys, values);

    verify(commands).evalsha(luaScript.getSha(), scriptOutputType, keys.toArray(new String[0]), values.toArray(new String[0]));
    verify(commands, never()).eval(anyString(), any(), any(), any());
  }

  @Test
  void testExecuteScriptNotLoaded() {
    final RedisAdvancedClusterCommands<String, String> commands = mock(RedisAdvancedClusterCommands.class);
    final FaultTolerantRedisClusterClient mockCluster = RedisClusterHelper.builder().stringCommands(commands).build();

    final String script = "return redis.call(\"SET\", KEYS[1], ARGV[1])";
    final ScriptOutputType scriptOutputType = ScriptOutputType.VALUE;
    final List<String> keys = List.of("key");
    final List<String> values = List.of("value");

    when(commands.evalsha(any(), any(), any(), any())).thenThrow(new RedisNoScriptException("OH NO"));

    final ClusterLuaScript luaScript = new ClusterLuaScript(mockCluster, script, scriptOutputType);
    luaScript.execute(keys, values);

    verify(commands).eval(script, scriptOutputType, keys.toArray(new String[0]), values.toArray(new String[0]));
    verify(commands).evalsha(luaScript.getSha(), scriptOutputType, keys.toArray(new String[0]), values.toArray(new String[0]));
  }

  @Test
  void testExecuteBinaryScriptNotLoaded() {
    final RedisAdvancedClusterCommands<String, String> stringCommands = mock(RedisAdvancedClusterCommands.class);
    final RedisAdvancedClusterCommands<byte[], byte[]> binaryCommands = mock(RedisAdvancedClusterCommands.class);
    final FaultTolerantRedisClusterClient mockCluster = RedisClusterHelper.builder()
        .stringCommands(stringCommands)
        .binaryCommands(binaryCommands)
        .build();

    final String script = "return redis.call(\"SET\", KEYS[1], ARGV[1])";
    final ScriptOutputType scriptOutputType = ScriptOutputType.VALUE;
    final List<byte[]> keys = List.of("key".getBytes(StandardCharsets.UTF_8));
    final List<byte[]> values = List.of("value".getBytes(StandardCharsets.UTF_8));

    when(binaryCommands.evalsha(any(), any(), any(), any())).thenThrow(new RedisNoScriptException("OH NO"));

    final ClusterLuaScript luaScript = new ClusterLuaScript(mockCluster, script, scriptOutputType);
    luaScript.executeBinary(keys, values);

    verify(binaryCommands).eval(script, scriptOutputType, keys.toArray(new byte[0][]), values.toArray(new byte[0][]));
    verify(binaryCommands).evalsha(luaScript.getSha(), scriptOutputType, keys.toArray(new byte[0][]),
        values.toArray(new byte[0][]));
  }

  @Test
  void testExecuteBinaryAsyncScriptNotLoaded() throws Exception {
    final RedisAdvancedClusterAsyncCommands<byte[], byte[]> binaryAsyncCommands =
        mock(RedisAdvancedClusterAsyncCommands.class);
    final FaultTolerantRedisClusterClient mockCluster =
        RedisClusterHelper.builder().binaryAsyncCommands(binaryAsyncCommands).build();

    final String script = "return redis.call(\"SET\", KEYS[1], ARGV[1])";
    final ScriptOutputType scriptOutputType = ScriptOutputType.VALUE;
    final List<byte[]> keys = List.of("key".getBytes(StandardCharsets.UTF_8));
    final List<byte[]> values = List.of("value".getBytes(StandardCharsets.UTF_8));

    final AsyncCommand<?, ?, ?> evalShaFailure = new AsyncCommand<>(mock(RedisCommand.class));
    evalShaFailure.completeExceptionally(new RedisNoScriptException("OH NO"));

    final AsyncCommand<?, ?, ?> evalSuccess = new AsyncCommand<>(mock(RedisCommand.class));
    evalSuccess.complete();

    when(binaryAsyncCommands.evalsha(any(), any(), any(), any())).thenReturn((RedisFuture<Object>) evalShaFailure);
    when(binaryAsyncCommands.eval(anyString(), any(), any(), any())).thenReturn((RedisFuture<Object>) evalSuccess);

    final ClusterLuaScript luaScript = new ClusterLuaScript(mockCluster, script, scriptOutputType);
    luaScript.executeBinaryAsync(keys, values).get(5, TimeUnit.SECONDS);

    verify(binaryAsyncCommands).eval(script, scriptOutputType, keys.toArray(new byte[0][]),
        values.toArray(new byte[0][]));
    verify(binaryAsyncCommands).evalsha(luaScript.getSha(), scriptOutputType, keys.toArray(new byte[0][]),
        values.toArray(new byte[0][]));
  }

  @Test
  void testExecuteBinaryReactiveScriptNotLoaded() {
    final RedisAdvancedClusterReactiveCommands<byte[], byte[]> binaryReactiveCommands =
        mock(RedisAdvancedClusterReactiveCommands.class);
    final FaultTolerantRedisClusterClient mockCluster = RedisClusterHelper.builder()
        .binaryReactiveCommands(binaryReactiveCommands).build();

    final String script = "return redis.call(\"SET\", KEYS[1], ARGV[1])";
    final ScriptOutputType scriptOutputType = ScriptOutputType.VALUE;
    final List<byte[]> keys = List.of("key".getBytes(StandardCharsets.UTF_8));
    final List<byte[]> values = List.of("value".getBytes(StandardCharsets.UTF_8));

    when(binaryReactiveCommands.evalsha(any(), any(), any(), any()))
        .thenReturn(Flux.error(new RedisNoScriptException("OH NO")));
    when(binaryReactiveCommands.eval(anyString(), any(), any(), any())).thenReturn(Flux.just("ok"));

    final ClusterLuaScript luaScript = new ClusterLuaScript(mockCluster, script, scriptOutputType);
    luaScript.executeBinaryReactive(keys, values).blockLast(Duration.ofSeconds(5));

    verify(binaryReactiveCommands).eval(script, scriptOutputType, keys.toArray(new byte[0][]),
        values.toArray(new byte[0][]));
    verify(binaryReactiveCommands).evalsha(luaScript.getSha(), scriptOutputType, keys.toArray(new byte[0][]),
        values.toArray(new byte[0][]));
  }

  @ParameterizedTest
  @EnumSource(ExecuteMode.class)
  void testExecuteRealCluster(final ExecuteMode mode) throws Exception {
    REDIS_CLUSTER_EXTENSION.getRedisCluster().withCluster(c -> c.sync().scriptFlush(FlushMode.SYNC));
    REDIS_CLUSTER_EXTENSION.getRedisCluster().withCluster(c -> c.sync().configResetstat());

    final ClusterLuaScript script = new ClusterLuaScript(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        "return 2;",
        ScriptOutputType.INTEGER);

    for (int i = 0; i < 7; i++) {
      final long actual = switch (mode) {
        case SYNC -> (long) script.execute(Collections.emptyList(), Collections.emptyList());
        case ASYNC ->
            (long) script.executeAsync(Collections.emptyList(), Collections.emptyList()).get(5, TimeUnit.SECONDS);
        case REACTIVE -> (long) script.executeReactive(Collections.emptyList(), Collections.emptyList())
            .blockLast(Duration.ofSeconds(5));
      };

      assertEquals(2L, actual);
    }

    final int evalCount = REDIS_CLUSTER_EXTENSION.getRedisCluster().withCluster(connection -> {
      final String commandStats = connection.sync().info("commandstats");

      // We're looking for (and parsing) a line in the command stats that looks like:
      //
      // ```
      // cmdstat_eval:calls=1,usec=44,usec_per_call=44.00
      // ```
      return Arrays.stream(commandStats.split("\\n"))
          .filter(line -> line.startsWith("cmdstat_eval:"))
          .map(String::trim)
          .map(evalLine -> Arrays.stream(evalLine.substring(evalLine.indexOf(':') + 1).split(","))
              .filter(pair -> pair.startsWith("calls="))
              .map(callsPair -> Integer.parseInt(callsPair.substring(callsPair.indexOf('=') + 1)))
              .findFirst()
              .orElse(0))
          .findFirst()
          .orElse(0);
    });

    assertEquals(1, evalCount);
  }

  private enum ExecuteMode {
    SYNC,
    ASYNC,
    REACTIVE
  }

}
