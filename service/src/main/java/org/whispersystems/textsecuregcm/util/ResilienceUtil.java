/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.github.resilience4j.micrometer.tagged.TaggedRetryMetrics;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.lettuce.core.RedisCommandTimeoutException;
import io.micrometer.core.instrument.Metrics;
import org.apache.commons.lang3.RandomStringUtils;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;

public class ResilienceUtil {

  private static final CircuitBreakerRegistry CIRCUIT_BREAKER_REGISTRY =
      CircuitBreakerRegistry.of(new CircuitBreakerConfiguration().toCircuitBreakerConfig());

  private static final RetryRegistry RETRY_REGISTRY =
      RetryRegistry.of(new RetryConfiguration().toRetryConfigBuilder().build());

  // Include a random suffix to avoid accidental collisions
  private static final String GENERAL_REDIS_CONFIGURATION_NAME =
      "redis-general-" + RandomStringUtils.insecure().nextAlphanumeric(8);

  static {
    setGeneralRedisRetryConfiguration(new RetryConfiguration());

    TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(CIRCUIT_BREAKER_REGISTRY)
        .bindTo(Metrics.globalRegistry);

    TaggedRetryMetrics.ofRetryRegistry(RETRY_REGISTRY)
        .bindTo(Metrics.globalRegistry);
  }

  public static CircuitBreakerRegistry getCircuitBreakerRegistry() {
    return CIRCUIT_BREAKER_REGISTRY;
  }

  public static RetryRegistry getRetryRegistry() {
    return RETRY_REGISTRY;
  }

  public static void setGeneralRedisRetryConfiguration(final RetryConfiguration retryConfiguration) {
    RETRY_REGISTRY.addConfiguration(GENERAL_REDIS_CONFIGURATION_NAME, retryConfiguration.toRetryConfigBuilder()
        .retryOnException(throwable -> throwable instanceof RedisCommandTimeoutException)
        .build());
  }

  /// Returns a `Retry` instance with a default configuration suitable for general Redis operations.
  ///
  /// @param name The name of this `Retry`. Calls to this method with the same name will return the same `Retry`
  /// instance, and the name is used to identify metrics tied to the returned `Retry` instance.
  public static Retry getGeneralRedisRetry(final String name) {
    return RETRY_REGISTRY.retry(name, GENERAL_REDIS_CONFIGURATION_NAME);
  }
}
