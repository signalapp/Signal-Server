/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.lettuce.core.ScriptOutputType;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.util.TestClock;

class DynamicRateLimiterTest {

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
    final DynamicRateLimiter rateLimiter = new DynamicRateLimiter(
        "test",
        () -> new RateLimiterConfig(1, Duration.ofHours(1), failOpen),
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
    final DynamicRateLimiter rateLimiter = new DynamicRateLimiter(
        "test",
        () -> new RateLimiterConfig(1, Duration.ofHours(1), failOpen),
        validateRateLimitScript,
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        CLOCK);

    final String key = RandomStringUtils.insecure().nextAlphanumeric(16);

    assertDoesNotThrow(() -> rateLimiter.validateAsync(key).toCompletableFuture().join());
    final CompletionException completionException =
        assertThrows(CompletionException.class, () -> rateLimiter.validateAsync(key).toCompletableFuture().join());

    assertInstanceOf(RateLimitExceededException.class, completionException.getCause());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void validateFailOpen(final boolean failOpen) {
    final ClusterLuaScript failingScript = mock(ClusterLuaScript.class);
    when(failingScript.execute(any(), any())).thenThrow(new RuntimeException("OH NO"));

    final DynamicRateLimiter rateLimiter = new DynamicRateLimiter(
        "test",
        () -> new RateLimiterConfig(1, Duration.ofHours(1), failOpen),
        failingScript,
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        CLOCK);

    final String key = RandomStringUtils.insecure().nextAlphanumeric(16);

    if (failOpen) {
      assertDoesNotThrow(() -> rateLimiter.validate(key));
    } else {
      assertThrows(RuntimeException.class, () -> rateLimiter.validate(key));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void validateFailOpenAsync(final boolean failOpen) {
    final ClusterLuaScript failingScript = mock(ClusterLuaScript.class);
    when(failingScript.executeAsync(any(), any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("OH NO")));

    final DynamicRateLimiter rateLimiter = new DynamicRateLimiter(
        "test",
        () -> new RateLimiterConfig(1, Duration.ofHours(1), failOpen),
        failingScript,
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        CLOCK);

    final String key = RandomStringUtils.insecure().nextAlphanumeric(16);

    if (failOpen) {
      assertDoesNotThrow(() -> rateLimiter.validate(key));
    } else {
      final CompletionException completionException =
          assertThrows(CompletionException.class, () -> rateLimiter.validateAsync(key).toCompletableFuture().join());

      assertInstanceOf(RuntimeException.class, completionException.getCause());
    }
  }

  @Test
  void configChange_ReduceRefillRate() {
    final AtomicReference<Duration> refillRate = new AtomicReference<>(Duration.ofMinutes(5));
    final DynamicRateLimiter rateLimiter = new DynamicRateLimiter(
        "test",
        () -> new RateLimiterConfig(1, refillRate.get(), false),
        validateRateLimitScript,
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        CLOCK);

    final String key = RandomStringUtils.insecure().nextAlphanumeric(16);

    assertDoesNotThrow(() -> rateLimiter.validate(key));
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.validate(key));

    CLOCK.pin(CLOCK.instant().plus(Duration.ofMinutes(1)));
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.validate(key));

    refillRate.set(Duration.ofMinutes(1));
    assertDoesNotThrow(() -> rateLimiter.validate(key));
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.validate(key));
  }

  @Test
  void configChange_IncreaseRefillRate() {
    final AtomicReference<Duration> refillRate = new AtomicReference<>(Duration.ofMinutes(5));
    final DynamicRateLimiter rateLimiter = new DynamicRateLimiter(
        "test",
        () -> new RateLimiterConfig(1, refillRate.get(), false),
        validateRateLimitScript,
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        CLOCK);

    final String key = RandomStringUtils.insecure().nextAlphanumeric(16);

    assertDoesNotThrow(() -> rateLimiter.validate(key));
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.validate(key));

    CLOCK.pin(CLOCK.instant().plus(Duration.ofMinutes(5)));
    assertTrue(rateLimiter.hasAvailablePermits(key, 1));

    refillRate.set(Duration.ofMinutes(10));
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.validate(key));

    CLOCK.pin(CLOCK.instant().plus(Duration.ofMinutes(5)));
    assertDoesNotThrow(() -> rateLimiter.validate(key));
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.validate(key));
  }

  @Test
  void configChange_ReduceBucketSize() {
    final AtomicInteger bucketSize = new AtomicInteger(5);
    final DynamicRateLimiter rateLimiter = new DynamicRateLimiter(
        "test",
        () -> new RateLimiterConfig(bucketSize.get(), Duration.ofMinutes(1), false),
        validateRateLimitScript,
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        CLOCK);

    final String key = RandomStringUtils.insecure().nextAlphanumeric(16);

    assertDoesNotThrow(() -> rateLimiter.validate(key));
    assertTrue(rateLimiter.hasAvailablePermits(key, 4));
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.validate(key, 5));

    bucketSize.set(1);
    // Changing the bucket size doesn't spend the tokens remaining in existing buckets, but does
    // effectively make those buckets overflow if it got smaller. There were 4 tokens available
    // before, so changing the bucket size to 1 effectively means there is 1 token left, not 0
    assertTrue(rateLimiter.hasAvailablePermits(key, 1));
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.validate(key, 2));
  }

  @Test
  void configChange_IncreaseBucketSize() {
    final AtomicInteger bucketSize = new AtomicInteger(5);
    final DynamicRateLimiter rateLimiter = new DynamicRateLimiter(
        "test",
        () -> new RateLimiterConfig(bucketSize.get(), Duration.ofMinutes(1), false),
        validateRateLimitScript,
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        CLOCK);

    final String key = RandomStringUtils.insecure().nextAlphanumeric(16);

    assertDoesNotThrow(() -> rateLimiter.validate(key));
    assertTrue(rateLimiter.hasAvailablePermits(key, 4));
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.validate(key, 5));

    bucketSize.set(10);
    // Increasing the bucket size doesn't retroactively refill buckets in redis, so we have to wait
    // until the bucket fills up
    CLOCK.pin(CLOCK.instant().plus(Duration.ofMinutes(10)));
    assertTrue(rateLimiter.hasAvailablePermits(key, 10));
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.validate(key, 11));
  }

  @Test
  void configChange_enableFailOpen() {
    final ClusterLuaScript failingScript = mock(ClusterLuaScript.class);
    when(failingScript.execute(any(), any())).thenThrow(new RuntimeException("OH NO"));

    final AtomicBoolean failOpen = new AtomicBoolean(false);
    final DynamicRateLimiter rateLimiter = new DynamicRateLimiter(
        "test",
        () -> new RateLimiterConfig(1, Duration.ofMinutes(1), failOpen.get()),
        failingScript,
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        CLOCK);

    final String key = RandomStringUtils.insecure().nextAlphanumeric(16);

    assertThrows(RuntimeException.class, () -> rateLimiter.validate(key));

    failOpen.set(true);

    assertDoesNotThrow(() -> rateLimiter.validate(key));
  }

  @Test
  void configChange_disableFailOpen() {
    final ClusterLuaScript failingScript = mock(ClusterLuaScript.class);
    when(failingScript.execute(any(), any())).thenThrow(new RuntimeException("OH NO"));

    final AtomicBoolean failOpen = new AtomicBoolean(true);
    final DynamicRateLimiter rateLimiter = new DynamicRateLimiter(
        "test",
        () -> new RateLimiterConfig(1, Duration.ofMinutes(1), failOpen.get()),
        failingScript,
        REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        CLOCK);

    final String key = RandomStringUtils.insecure().nextAlphanumeric(16);

    assertDoesNotThrow(() -> rateLimiter.validate(key));

    failOpen.set(false);

    assertThrows(RuntimeException.class, () -> rateLimiter.validate(key));
  }
}
