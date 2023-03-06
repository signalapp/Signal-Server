/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.lettuce.core.ScriptOutputType;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.MutableClock;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.redis.RedisLuaScriptSandbox;
import org.whispersystems.textsecuregcm.util.redis.SimpleCacheCommandsHandler;

public class RateLimitersLuaScriptTest {

  @RegisterExtension
  private static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private final DynamicConfiguration configuration = mock(DynamicConfiguration.class);

  private final MutableClock clock = MockUtils.mutableClock(0);

  private final RedisLuaScriptSandbox sandbox = RedisLuaScriptSandbox.fromResource(
      "lua/validate_rate_limit.lua",
      ScriptOutputType.INTEGER);

  private final SimpleCacheCommandsHandler redisCommandsHandler = new SimpleCacheCommandsHandler(clock);

  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfig =
      MockUtils.buildMock(DynamicConfigurationManager.class, cfg -> when(cfg.getConfiguration()).thenReturn(configuration));

  @Test
  public void testWithEmbeddedRedis() throws Exception {
    final RateLimiters.For descriptor = RateLimiters.For.REGISTRATION;
    final FaultTolerantRedisCluster redisCluster = REDIS_CLUSTER_EXTENSION.getRedisCluster();
    final RateLimiters limiters = new RateLimiters(
        Map.of(descriptor.id(), new RateLimiterConfig(60, 60)),
        dynamicConfig,
        RateLimiters.defaultScript(redisCluster),
        redisCluster,
        Clock.systemUTC());

    final RateLimiter rateLimiter = limiters.forDescriptor(descriptor);
    rateLimiter.validate("test", 25);
    rateLimiter.validate("test", 25);
    assertThrows(Exception.class, () -> rateLimiter.validate("test", 25));
  }

  @Test
  public void testLuaBucketConfigurationUpdates() throws Exception {
    final String key = "key1";
    clock.setTimeMillis(0);
    long result = (long) sandbox.execute(
        List.of(key),
        scriptArgs(1000, 1, 1, true),
        redisCommandsHandler
    );
    assertEquals(0L, result);
    assertEquals(1000L, decodeBucket(key).orElseThrow().bucketSize);

    // now making a check-only call, but changing the bucket size
    result = (long) sandbox.execute(
        List.of(key),
        scriptArgs(2000, 1, 1, false),
        redisCommandsHandler
    );
    assertEquals(0L, result);
    assertEquals(2000L, decodeBucket(key).orElseThrow().bucketSize);
  }

  @Test
  public void testLuaUpdatesTokenBucket() throws Exception {
    final String key = "key1";
    clock.setTimeMillis(0);
    long result = (long) sandbox.execute(
        List.of(key),
        scriptArgs(1000, 1, 200, true),
        redisCommandsHandler
    );
    assertEquals(0L, result);
    assertEquals(800L, decodeBucket(key).orElseThrow().spaceRemaining);

    // 50 tokens replenished, acquiring 100 more, should end up with 750 available
    clock.setTimeMillis(50);
    result = (long) sandbox.execute(
        List.of(key),
        scriptArgs(1000, 1, 100, true),
        redisCommandsHandler
    );
    assertEquals(0L, result);
    assertEquals(750L, decodeBucket(key).orElseThrow().spaceRemaining);

    // now checking without an update, should not affect the count
    result = (long) sandbox.execute(
        List.of(key),
        scriptArgs(1000, 1, 100, false),
        redisCommandsHandler
    );
    assertEquals(0L, result);
    assertEquals(750L, decodeBucket(key).orElseThrow().spaceRemaining);
  }

  private Optional<TokenBucket> decodeBucket(final String key) {
    try {
      final String json = redisCommandsHandler.get(key);
      return json == null
          ? Optional.empty()
          : Optional.of(SystemMapper.jsonMapper().readValue(json, TokenBucket.class));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> scriptArgs(
      final long bucketSize,
      final long ratePerMillis,
      final long requestedAmount,
      final boolean useTokens) {
    return List.of(
        String.valueOf(bucketSize),
        String.valueOf(ratePerMillis),
        String.valueOf(clock.millis()),
        String.valueOf(requestedAmount),
        String.valueOf(useTokens)
    );
  }

  private record TokenBucket(long bucketSize, long leakRatePerMillis, long spaceRemaining, long lastUpdateTimeMillis) {
  }
}
