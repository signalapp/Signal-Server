/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.lettuce.core.RedisException;
import io.lettuce.core.ScriptOutputType;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
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
    final Map<String, RateLimiterConfig> limiterConfig = Map.of(descriptor.id(), new RateLimiterConfig(60, Duration.ofSeconds(1), false));
    when(configuration.getLimits()).thenReturn(limiterConfig);

    final FaultTolerantRedisClusterClient redisCluster = REDIS_CLUSTER_EXTENSION.getRedisCluster();
    final RateLimiters limiters = new RateLimiters(
        dynamicConfig,
        RateLimiters.defaultScript(redisCluster),
        redisCluster,
        Clock.systemUTC());

    final RateLimiter rateLimiter = limiters.forDescriptor(descriptor);
    rateLimiter.validate("test", 25);
    rateLimiter.validate("test", 25);
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.validate("test", 25));
  }

  @Test
  public void testTtl() throws Exception {
    final RateLimiters.For descriptor = RateLimiters.For.REGISTRATION;
    final Map<String, RateLimiterConfig> limiterConfig = Map.of(descriptor.id(), new RateLimiterConfig(1000, Duration.ofSeconds(1), false));
    when(configuration.getLimits()).thenReturn(limiterConfig);

    final FaultTolerantRedisClusterClient redisCluster = REDIS_CLUSTER_EXTENSION.getRedisCluster();
    final RateLimiters limiters = new RateLimiters(
        dynamicConfig,
        RateLimiters.defaultScript(redisCluster),
        redisCluster,
        Clock.systemUTC());

    final RateLimiter rateLimiter = limiters.forDescriptor(descriptor);
    rateLimiter.validate("test", 200);
    // after using 200 tokens, we expect 200 seconds to refill, so the TTL should be under 200000
    final long ttl = redisCluster.withCluster(c -> c.sync().ttl("test"));
    assertTrue(ttl <= 200000);
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
    assertEquals(800L, decodeBucket(key).orElseThrow().tokensRemaining);

    // 50 tokens replenished, acquiring 100 more, should end up with 750 available
    clock.setTimeMillis(50);
    result = (long) sandbox.execute(
        List.of(key),
        scriptArgs(1000, 1, 100, true),
        redisCommandsHandler
    );
    assertEquals(0L, result);
    assertEquals(750L, decodeBucket(key).orElseThrow().tokensRemaining);

    // now checking without an update, should not affect the count
    result = (long) sandbox.execute(
        List.of(key),
        scriptArgs(1000, 1, 100, false),
        redisCommandsHandler
    );
    assertEquals(0L, result);
    assertEquals(750L, decodeBucket(key).orElseThrow().tokensRemaining);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testFailOpen(final boolean failOpen) {
    final RateLimiters.For descriptor = RateLimiters.For.REGISTRATION;
    final FaultTolerantRedisClusterClient redisCluster = mock(FaultTolerantRedisClusterClient.class);

    final Map<String, RateLimiterConfig> limiterConfig = Map.of(descriptor.id(), new RateLimiterConfig(1, Duration.ofSeconds(1), failOpen));
    when(configuration.getLimits()).thenReturn(limiterConfig);

    final RateLimiters limiters = new RateLimiters(
        dynamicConfig,
        RateLimiters.defaultScript(redisCluster),
        redisCluster,
        Clock.systemUTC());
    when(redisCluster.withCluster(any())).thenThrow(new RedisException("fail"));
    final RateLimiter rateLimiter = limiters.forDescriptor(descriptor);

    if (failOpen) {
      assertDoesNotThrow(() -> rateLimiter.validate("test", 200));
    } else {
      assertThrows(RedisException.class, () -> rateLimiter.validate("test", 200));
    }
  }

  private String serializeToOldBucketValueFormat(
      final long bucketSize,
      final long leakRatePerMillis,
      final long spaceRemaining,
      final long lastUpdateTimeMillis) {
    try {
      return SystemMapper.jsonMapper().writeValueAsString(Map.of(
          "bucketSize", bucketSize,
          "leakRatePerMillis", leakRatePerMillis,
          "spaceRemaining", spaceRemaining,
          "lastUpdateTimeMillis", lastUpdateTimeMillis
      ));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private Optional<TokenBucket> decodeBucket(final String key) {
    final Object[] fields = redisCommandsHandler.hmget(key, List.of("s", "t"));
    return fields[0] == null
        ? Optional.empty()
        : Optional.of(new TokenBucket(
            Double.valueOf(fields[0].toString()).longValue(), Double.valueOf(fields[1].toString()).longValue()));
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

  private record TokenBucket(long tokensRemaining, long lastUpdateTimeMillis) {
  }
}
