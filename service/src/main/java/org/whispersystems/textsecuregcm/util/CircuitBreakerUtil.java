/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static com.codahale.metrics.MetricRegistry.name;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.github.resilience4j.micrometer.tagged.TaggedRetryMetrics;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.lettuce.core.RedisCommandTimeoutException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.apache.commons.lang3.RandomStringUtils;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;

public class CircuitBreakerUtil {

  private static final String CIRCUIT_BREAKER_CALL_COUNTER_NAME = name(CircuitBreakerUtil.class, "breaker", "call");
  private static final String CIRCUIT_BREAKER_STATE_GAUGE_NAME = name(CircuitBreakerUtil.class, "breaker", "state");
  private static final String RETRY_CALL_COUNTER_NAME = name(CircuitBreakerUtil.class, "retry", "call");

  private static final String BREAKER_NAME_TAG_NAME = "breakerName";
  private static final String OUTCOME_TAG_NAME = "outcome";

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

  /// @deprecated [CircuitBreakerRegistry] maintains its own set of metrics, and manually managing them is unnecessary
  @Deprecated(forRemoval = true)
  public static void registerMetrics(CircuitBreaker circuitBreaker, Class<?> clazz, Tags additionalTags) {
    final String breakerName = clazz.getSimpleName() + "/" + circuitBreaker.getName();

    final Counter successCounter = Metrics.counter(CIRCUIT_BREAKER_CALL_COUNTER_NAME,
        additionalTags.and(
            BREAKER_NAME_TAG_NAME, breakerName,
            OUTCOME_TAG_NAME, "success"));

    final Counter failureCounter = Metrics.counter(CIRCUIT_BREAKER_CALL_COUNTER_NAME,
        additionalTags.and(
            BREAKER_NAME_TAG_NAME, breakerName,
            OUTCOME_TAG_NAME, "failure"));

    final Counter unpermittedCounter = Metrics.counter(CIRCUIT_BREAKER_CALL_COUNTER_NAME,
        additionalTags.and(BREAKER_NAME_TAG_NAME, breakerName,
            OUTCOME_TAG_NAME, "unpermitted"));

    circuitBreaker.getEventPublisher().onSuccess(event -> {
      successCounter.increment();
    });

    circuitBreaker.getEventPublisher().onError(event -> {
      failureCounter.increment();
    });

    circuitBreaker.getEventPublisher().onCallNotPermitted(event -> {
      unpermittedCounter.increment();
    });

    Metrics.gauge(CIRCUIT_BREAKER_STATE_GAUGE_NAME,
        Tags.of(Tag.of(BREAKER_NAME_TAG_NAME, circuitBreaker.getName())),
        circuitBreaker, breaker -> breaker.getState().getOrder());
  }

  /// @deprecated [RetryRegistry] maintains its own set of metrics, and manually managing them is unnecessary
  @Deprecated(forRemoval = true)
  public static void registerMetrics(Retry retry, Class<?> clazz) {
    final String retryName = clazz.getSimpleName() + "/" + retry.getName();

    final Counter successCounter = Metrics.counter(RETRY_CALL_COUNTER_NAME,
        BREAKER_NAME_TAG_NAME, retryName,
        OUTCOME_TAG_NAME, "success");

    final Counter retryCounter = Metrics.counter(RETRY_CALL_COUNTER_NAME,
        BREAKER_NAME_TAG_NAME, retryName,
        OUTCOME_TAG_NAME, "retry");

    final Counter errorCounter = Metrics.counter(RETRY_CALL_COUNTER_NAME,
        BREAKER_NAME_TAG_NAME, retryName,
        OUTCOME_TAG_NAME, "error");

    final Counter ignoredErrorCounter = Metrics.counter(RETRY_CALL_COUNTER_NAME,
        BREAKER_NAME_TAG_NAME, retryName,
        OUTCOME_TAG_NAME, "ignored_error");

    retry.getEventPublisher().onSuccess(event -> {
      successCounter.increment();
    });

    retry.getEventPublisher().onRetry(event -> {
      retryCounter.increment();
    });

    retry.getEventPublisher().onError(event -> {
      errorCounter.increment();
    });

    retry.getEventPublisher().onIgnoredError(event -> {
      ignoredErrorCounter.increment();
    });
  }

}
