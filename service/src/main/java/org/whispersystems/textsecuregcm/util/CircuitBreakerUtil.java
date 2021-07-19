/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import static com.codahale.metrics.MetricRegistry.name;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

public class CircuitBreakerUtil {

  private static final String CIRCUIT_BREAKER_CALL_COUNTER_NAME = name(CircuitBreakerUtil.class, "breaker", "call");
  private static final String CIRCUIT_BREAKER_STATE_GAUGE_NAME = name(CircuitBreakerUtil.class, "breaker", "state");
  private static final String RETRY_CALL_COUNTER_NAME = name(CircuitBreakerUtil.class, "retry", "call");

  private static final String NAME_TAG_NAME = "name";
  private static final String OUTCOME_TAG_NAME = "outcome";

  public static void registerMetrics(MetricRegistry metricRegistry, CircuitBreaker circuitBreaker, Class<?> clazz) {
    Meter successMeter     = metricRegistry.meter(name(clazz, circuitBreaker.getName(), "success"    ));
    Meter failureMeter     = metricRegistry.meter(name(clazz, circuitBreaker.getName(), "failure"    ));
    Meter unpermittedMeter = metricRegistry.meter(name(clazz, circuitBreaker.getName(), "unpermitted"));

    final String breakerName = clazz.getSimpleName() + "/" + circuitBreaker.getName();

    final Counter successCounter = Metrics.counter(CIRCUIT_BREAKER_CALL_COUNTER_NAME,
        NAME_TAG_NAME, breakerName,
        OUTCOME_TAG_NAME, "success");

    final Counter failureCounter = Metrics.counter(CIRCUIT_BREAKER_CALL_COUNTER_NAME,
        NAME_TAG_NAME, breakerName,
        OUTCOME_TAG_NAME, "failure");

    final Counter unpermittedCounter = Metrics.counter(CIRCUIT_BREAKER_CALL_COUNTER_NAME,
        NAME_TAG_NAME, breakerName,
        OUTCOME_TAG_NAME, "unpermitted");

    circuitBreaker.getEventPublisher().onSuccess(event -> {
      successMeter.mark();
      successCounter.increment();
    });

    circuitBreaker.getEventPublisher().onError(event -> {
      failureMeter.mark();
      failureCounter.increment();
    });

    circuitBreaker.getEventPublisher().onCallNotPermitted(event -> {
      unpermittedMeter.mark();
      unpermittedCounter.increment();
    });

    metricRegistry.gauge(name(clazz, circuitBreaker.getName(), "state"), () -> ()-> circuitBreaker.getState().getOrder());

    Metrics.gauge(CIRCUIT_BREAKER_STATE_GAUGE_NAME,
        Tags.of(Tag.of(NAME_TAG_NAME, circuitBreaker.getName())),
        circuitBreaker, breaker -> breaker.getState().getOrder());
  }

  public static void registerMetrics(MetricRegistry metricRegistry, Retry retry, Class<?> clazz) {
    Meter successMeter      = metricRegistry.meter(name(clazz, retry.getName(), "success"      ));
    Meter retryMeter        = metricRegistry.meter(name(clazz, retry.getName(), "retry"        ));
    Meter errorMeter        = metricRegistry.meter(name(clazz, retry.getName(), "error"        ));
    Meter ignoredErrorMeter = metricRegistry.meter(name(clazz, retry.getName(), "ignored_error"));

    final String retryName = clazz.getSimpleName() + "/" + retry.getName();

    final Counter successCounter = Metrics.counter(RETRY_CALL_COUNTER_NAME,
        NAME_TAG_NAME, retryName,
        OUTCOME_TAG_NAME, "success");

    final Counter retryCounter = Metrics.counter(RETRY_CALL_COUNTER_NAME,
        NAME_TAG_NAME, retryName,
        OUTCOME_TAG_NAME, "retry");

    final Counter errorCounter = Metrics.counter(RETRY_CALL_COUNTER_NAME,
        NAME_TAG_NAME, retryName,
        OUTCOME_TAG_NAME, "error");

    final Counter ignoredErrorCounter = Metrics.counter(RETRY_CALL_COUNTER_NAME,
        NAME_TAG_NAME, retryName,
        OUTCOME_TAG_NAME, "ignored_error");

    retry.getEventPublisher().onSuccess(event -> {
      successMeter.mark();
      successCounter.increment();
    });

    retry.getEventPublisher().onRetry(event -> {
      retryMeter.mark();
      retryCounter.increment();
    });

    retry.getEventPublisher().onError(event -> {
      errorMeter.mark();
      errorCounter.increment();
    });

    retry.getEventPublisher().onIgnoredError(event -> {
      ignoredErrorMeter.mark();
      ignoredErrorCounter.increment();
    });
  }

}
