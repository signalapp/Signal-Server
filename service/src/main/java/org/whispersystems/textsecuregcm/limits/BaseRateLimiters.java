/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static java.util.Objects.requireNonNull;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public abstract class BaseRateLimiters<T extends RateLimiterDescriptor> {

  private final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<T, RateLimiter> rateLimiterByDescriptor;

  private final Map<String, RateLimiterConfig> configs;


  protected BaseRateLimiters(
      final T[] values,
      final Map<String, RateLimiterConfig> configs,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final FaultTolerantRedisCluster cacheCluster) {
    this.configs = configs;
    this.rateLimiterByDescriptor = Arrays.stream(values)
        .map(descriptor -> Pair.of(
            descriptor,
            createForDescriptor(descriptor, configs, dynamicConfigurationManager, cacheCluster)))
        .collect(Collectors.toUnmodifiableMap(Pair::getKey, Pair::getValue));
  }

  public RateLimiter forDescriptor(final T handle) {
    return requireNonNull(rateLimiterByDescriptor.get(handle));
  }

  public void validateValuesAndConfigs() {
    final Set<String> ids = rateLimiterByDescriptor.keySet().stream()
        .map(RateLimiterDescriptor::id)
        .collect(Collectors.toSet());
    for (final String key: configs.keySet()) {
      if (!ids.contains(key)) {
        final String message = String.format(
            "Static configuration has an unexpected field '%s' that doesn't match any RateLimiterDescriptor",
            key
        );
        logger.error(message);
        throw new IllegalArgumentException(message);
      }
    }
  }

  private static RateLimiter createForDescriptor(
      final RateLimiterDescriptor descriptor,
      final Map<String, RateLimiterConfig> configs,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final FaultTolerantRedisCluster cacheCluster) {
    if (descriptor.isDynamic()) {
      final Supplier<RateLimiterConfig> configResolver = () -> {
        final RateLimiterConfig config = dynamicConfigurationManager.getConfiguration().getLimits().get(descriptor.id());
        return config != null
            ? config
            : configs.getOrDefault(descriptor.id(), descriptor.defaultConfig());
      };
      return new DynamicRateLimiter(descriptor.id(), configResolver, cacheCluster);
    }
    final RateLimiterConfig cfg = configs.getOrDefault(descriptor.id(), descriptor.defaultConfig());
    return new StaticRateLimiter(descriptor.id(), cfg, cacheCluster);
  }
}
