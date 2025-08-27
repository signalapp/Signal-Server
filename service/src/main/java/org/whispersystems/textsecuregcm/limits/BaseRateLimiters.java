/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static java.util.Objects.requireNonNull;

import io.lettuce.core.ScriptOutputType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public abstract class BaseRateLimiters<T extends RateLimiterDescriptor> {

  private final Map<T, RateLimiter> rateLimiterByDescriptor;

  protected BaseRateLimiters(
      final T[] values,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final ClusterLuaScript validateScript,
      final FaultTolerantRedisClusterClient cacheCluster,
      final ScheduledExecutorService retryExecutor,
      final Clock clock) {
    this.rateLimiterByDescriptor = Arrays.stream(values)
        .map(descriptor -> Pair.of(
            descriptor,
            createForDescriptor(descriptor, dynamicConfigurationManager, validateScript, cacheCluster, retryExecutor, clock)))
        .collect(Collectors.toUnmodifiableMap(Pair::getKey, Pair::getValue));
  }

  public RateLimiter forDescriptor(final T handle) {
    return requireNonNull(rateLimiterByDescriptor.get(handle));
  }

  protected static ClusterLuaScript defaultScript(final FaultTolerantRedisClusterClient cacheCluster) {
    try {
      return ClusterLuaScript.fromResource(
          cacheCluster, "lua/validate_rate_limit.lua", ScriptOutputType.INTEGER);
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to load rate limit validation script", e);
    }
  }

  private static RateLimiter createForDescriptor(
      final RateLimiterDescriptor descriptor,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final ClusterLuaScript validateScript,
      final FaultTolerantRedisClusterClient cacheCluster,
      final ScheduledExecutorService retryExecutor,
      final Clock clock) {
    final Supplier<RateLimiterConfig> configResolver =
        () -> dynamicConfigurationManager.getConfiguration().getLimits().getOrDefault(descriptor.id(), descriptor.defaultConfig());
    return new DynamicRateLimiter(descriptor.id(), configResolver, validateScript, cacheCluster, retryExecutor, clock);
  }
}
