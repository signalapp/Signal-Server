/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.lettuce.core.RedisCommandTimeoutException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.apache.commons.lang3.RandomStringUtils;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

public class ResilienceUtil {

  private static final CircuitBreakerRegistry CIRCUIT_BREAKER_REGISTRY =
      CircuitBreakerRegistry.of(new CircuitBreakerConfiguration().toCircuitBreakerConfig());

  private static final RetryRegistry RETRY_REGISTRY =
      RetryRegistry.of(new RetryConfiguration().toRetryConfigBuilder().build());

  private static final ConcurrentMap<String, Set<Meter.Id>> METER_IDS_BY_BREAKER_NAME = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Set<Meter.Id>> METER_IDS_BY_RETRY_NAME = new ConcurrentHashMap<>();

  private static final String BREAKER_CALL_COUNTER_NAME = MetricsUtil.name(ResilienceUtil.class, "breaker", "call");
  private static final String BREAKER_STATE_GAUGE_NAME = MetricsUtil.name(ResilienceUtil.class, "breaker", "state");
  private static final String RETRY_CALL_COUNTER_NAME = MetricsUtil.name(ResilienceUtil.class, "retry", "call");

  private static final String BREAKER_NAME_TAG_NAME = "breakerName";
  private static final String RETRY_NAME_TAG = "retryName";
  private static final String OUTCOME_TAG_NAME = "outcome";

  // Include a random suffix to avoid accidental collisions
  private static final String GENERAL_REDIS_CONFIGURATION_NAME =
      "redis-general-" + RandomStringUtils.insecure().nextAlphanumeric(8);

  static {
    setGeneralRedisRetryConfiguration(new RetryConfiguration());

    CIRCUIT_BREAKER_REGISTRY.getEventPublisher()
        .onEntryAdded(event -> addMetrics(event.getAddedEntry()))
        .onEntryRemoved(event -> removeMetrics(event.getRemovedEntry()))
        .onEntryReplaced(event -> {
          removeMetrics(event.getOldEntry());
          addMetrics(event.getNewEntry());
        });

    RETRY_REGISTRY.getEventPublisher()
        .onEntryAdded(event -> addMetrics(event.getAddedEntry()))
        .onEntryRemoved(event -> removeMetrics(event.getRemovedEntry()))
        .onEntryReplaced(event -> {
          removeMetrics(event.getOldEntry());
          addMetrics(event.getNewEntry());
        });
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

  /// Generates a standardized name for a `CircuitBreaker` or `Retry`.
  ///
  /// @param clazz the class to which the circuit breaker or retry belongs
  ///
  /// @return a standardized name for a `CircuitBreaker` or `Retry`
  public static String name(final Class<?> clazz) {
    return name(clazz, null);
  }

  /// Generates a standardized name for a `CircuitBreaker` or `Retry`.
  ///
  /// @param clazz the class to which the circuit breaker or retry belongs
  /// @param name the name of the circuit breaker or retry; may be `null``
  ///
  /// @return a standardized name for a `CircuitBreaker` or `Retry`
  public static String name(final Class<?> clazz, @Nullable final String name) {
    return name != null
        ? clazz.getSimpleName() + "/" + name
        : clazz.getSimpleName();
  }

  /// Returns a `Retry` instance with a default configuration suitable for general Redis operations.
  ///
  /// @param name The name of this `Retry`. Calls to this method with the same name will return the same `Retry`
  /// instance, and the name is used to identify metrics tied to the returned `Retry` instance.
  public static Retry getGeneralRedisRetry(final String name) {
    return RETRY_REGISTRY.retry("redis/" + name, GENERAL_REDIS_CONFIGURATION_NAME);
  }

  private static void addMetrics(final CircuitBreaker circuitBreaker) {
    // Remove previous meters before registering new ones
    final Set<Meter.Id> meterIds = new HashSet<>();
    final List<Tag> additionalTags = toTags(circuitBreaker.getTags());

    meterIds.add(Gauge.builder(BREAKER_STATE_GAUGE_NAME, circuitBreaker, breaker -> switch (breaker.getState()) {
          case OPEN, HALF_OPEN, FORCED_OPEN -> 1;
          default -> 0;
        })
        .tag(BREAKER_NAME_TAG_NAME, circuitBreaker.getName())
        .tags(additionalTags)
        .register(Metrics.globalRegistry)
        .getId());

    final Counter successCounter = Counter.builder(BREAKER_CALL_COUNTER_NAME)
        .tag(BREAKER_NAME_TAG_NAME, circuitBreaker.getName())
        .tag(OUTCOME_TAG_NAME, "success")
        .tags(additionalTags)
        .register(Metrics.globalRegistry);

    final Counter failureCounter = Counter.builder(BREAKER_CALL_COUNTER_NAME)
        .tag(BREAKER_NAME_TAG_NAME, circuitBreaker.getName())
        .tag(OUTCOME_TAG_NAME, "failure")
        .tags(additionalTags)
        .register(Metrics.globalRegistry);

    final Counter unpermittedCounter = Counter.builder(BREAKER_CALL_COUNTER_NAME)
        .tag(BREAKER_NAME_TAG_NAME, circuitBreaker.getName())
        .tag(OUTCOME_TAG_NAME, "unpermitted")
        .tags(additionalTags)
        .register(Metrics.globalRegistry);

    circuitBreaker.getEventPublisher()
        .onSuccess(_ -> successCounter.increment())
        .onError(_ -> failureCounter.increment())
        .onCallNotPermitted(_ -> unpermittedCounter.increment());

    meterIds.add(successCounter.getId());
    meterIds.add(failureCounter.getId());
    meterIds.add(unpermittedCounter.getId());

    METER_IDS_BY_BREAKER_NAME.put(circuitBreaker.getName(), meterIds);
  }

  private static void addMetrics(final Retry retry) {
    final Set<Meter.Id> meterIds = new HashSet<>();
    final List<Tag> additionalTags = toTags(retry.getTags());

    final Counter successCounter = Counter.builder(RETRY_CALL_COUNTER_NAME)
        .tag(RETRY_NAME_TAG, retry.getName())
        .tag(OUTCOME_TAG_NAME, "success")
        .tags(additionalTags)
        .register(Metrics.globalRegistry);

    final Counter retryCounter = Counter.builder(RETRY_CALL_COUNTER_NAME)
        .tag(RETRY_NAME_TAG, retry.getName())
        .tag(OUTCOME_TAG_NAME, "retry")
        .tags(additionalTags)
        .register(Metrics.globalRegistry);

    final Counter errorCounter = Counter.builder(RETRY_CALL_COUNTER_NAME)
        .tag(RETRY_NAME_TAG, retry.getName())
        .tag(OUTCOME_TAG_NAME, "error")
        .tags(additionalTags)
        .register(Metrics.globalRegistry);

    retry.getEventPublisher()
        .onSuccess(_ -> successCounter.increment())
        .onRetry(_ -> retryCounter.increment())
        .onError(_ -> errorCounter.increment());

    meterIds.add(successCounter.getId());
    meterIds.add(retryCounter.getId());
    meterIds.add(errorCounter.getId());

    METER_IDS_BY_RETRY_NAME.put(retry.getName(), meterIds);
  }

  private static void removeMetrics(final CircuitBreaker circuitBreaker) {
    removeMetrics(METER_IDS_BY_BREAKER_NAME.remove(circuitBreaker.getName()));
  }

  private static void removeMetrics(final Retry retry) {
    removeMetrics(METER_IDS_BY_RETRY_NAME.remove(retry.getName()));
  }

  private static void removeMetrics(@Nullable final Set<Meter.Id> meterIds) {
    if (meterIds != null) {
      meterIds.forEach(Metrics.globalRegistry::remove);
    }
  }

  private static List<Tag> toTags(final Map<String, String> tagMap) {
    return tagMap.entrySet().stream()
        .map(entry -> Tag.of(entry.getKey(), entry.getValue()))
        .toList();
  }
}
