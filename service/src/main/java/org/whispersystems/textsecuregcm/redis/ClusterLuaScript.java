/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterLuaScript {

  private final FaultTolerantRedisCluster redisCluster;
  private final ScriptOutputType scriptOutputType;
  private final String script;
  private final String sha;

  private static final String[] STRING_ARRAY = new String[0];
  private static final byte[][] BYTE_ARRAY_ARRAY = new byte[0][];

  private static final Logger log = LoggerFactory.getLogger(ClusterLuaScript.class);

  public static ClusterLuaScript fromResource(final FaultTolerantRedisCluster redisCluster,
      final String resource,
      final ScriptOutputType scriptOutputType) throws IOException {

    try (final InputStream inputStream = ClusterLuaScript.class.getClassLoader().getResourceAsStream(resource)) {
      if (inputStream == null) {
        throw new IllegalArgumentException("Script not found: " + resource);
      }

      return new ClusterLuaScript(redisCluster,
          new String(inputStream.readAllBytes(), StandardCharsets.UTF_8),
          scriptOutputType);
    }
  }

  @VisibleForTesting
  ClusterLuaScript(final FaultTolerantRedisCluster redisCluster,
      final String script,
      final ScriptOutputType scriptOutputType) {

    this.redisCluster = redisCluster;
    this.scriptOutputType = scriptOutputType;
    this.script = script;

    try {
      this.sha = Hex.encodeHexString(MessageDigest.getInstance("SHA-1").digest(script.getBytes(StandardCharsets.UTF_8)));
    } catch (final NoSuchAlgorithmException e) {
      // All Java implementations are required to support SHA-1, so this should never happen
      throw new AssertionError(e);
    }
  }

  @VisibleForTesting
  String getSha() {
    return sha;
  }

  public Object execute(final List<String> keys, final List<String> args) {
    return redisCluster.withCluster(connection ->
        execute(connection, keys.toArray(STRING_ARRAY), args.toArray(STRING_ARRAY)));
  }

  public Object executeBinary(final List<byte[]> keys, final List<byte[]> args) {
    return redisCluster.withBinaryCluster(connection ->
        execute(connection, keys.toArray(BYTE_ARRAY_ARRAY), args.toArray(BYTE_ARRAY_ARRAY)));
  }

  private <T> Object execute(final StatefulRedisClusterConnection<T, T> connection, final T[] keys, final T[] args) {
    try {
      try {
        return connection.sync().evalsha(sha, scriptOutputType, keys, args);
      } catch (final RedisNoScriptException e) {
        return connection.sync().eval(script, scriptOutputType, keys, args);
      }
    } catch (final Exception e) {
      log.warn("Failed to execute script", e);
      throw e;
    }
  }
}
