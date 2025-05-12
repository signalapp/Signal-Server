/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.lettuce.core.ScriptOutputType;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.util.TestClock;

class StaticRateLimiterTest {

  private ClusterLuaScript validateRateLimitScript;

  private static final TestClock CLOCK = TestClock.pinned(Instant.now());

  @RegisterExtension
  private static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @BeforeEach
  void setUp() throws IOException {
    validateRateLimitScript = ClusterLuaScript.fromResource(
        REDIS_CLUSTER_EXTENSION.getRedisCluster(), "lua/validate_rate_limit.lua", ScriptOutputType.INTEGER);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void validate(final boolean failOpen) {
    final StaticRateLimiter rateLimiter = new StaticRateLimiter("test",
        new RateLimiterConfig(1, Duration.ofHours(1), failOpen),
        validateRateLimitScript,
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        CLOCK);

    final String key = RandomStringUtils.insecure().nextAlphanumeric(16);

    assertDoesNotThrow(() -> rateLimiter.validate(key));
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.validate(key));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void validateAsync(final boolean failOpen) {
    final StaticRateLimiter rateLimiter = new StaticRateLimiter("test",
        new RateLimiterConfig(1, Duration.ofHours(1), failOpen),
        validateRateLimitScript,
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        CLOCK);

    final String key = RandomStringUtils.insecure().nextAlphanumeric(16);

    assertDoesNotThrow(() -> rateLimiter.validateAsync(key).toCompletableFuture().join());
    final CompletionException completionException =
        assertThrows(CompletionException.class, () -> rateLimiter.validateAsync(key).toCompletableFuture().join());

    assertInstanceOf(RateLimitExceededException.class, completionException.getCause());
  }
}
