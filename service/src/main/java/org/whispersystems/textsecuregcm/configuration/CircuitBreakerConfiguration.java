/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CircuitBreakerConfiguration {

  @JsonProperty
  @NotNull
  @Min(1)
  @Max(100)
  private int failureRateThreshold = 50;

  @JsonProperty
  @NotNull
  @Min(1)
  private int permittedNumberOfCallsInHalfOpenState = 10;

  @JsonProperty
  @NotNull
  @Min(1)
  private int slidingWindowSize = 100;

  @JsonProperty
  @NotNull
  @Min(1)
  private int slidingWindowMinimumNumberOfCalls = 100;

  @JsonProperty
  @NotNull
  private Duration waitDurationInOpenState = Duration.ofSeconds(10);

  @JsonProperty
  private List<String> ignoredExceptions = Collections.emptyList();


  public int getFailureRateThreshold() {
    return failureRateThreshold;
  }

  public int getPermittedNumberOfCallsInHalfOpenState() {
    return permittedNumberOfCallsInHalfOpenState;
  }

  public int getSlidingWindowSize() {
    return slidingWindowSize;
  }

  public int getSlidingWindowMinimumNumberOfCalls() {
    return slidingWindowMinimumNumberOfCalls;
  }

  public Duration getWaitDurationInOpenState() {
    return waitDurationInOpenState;
  }

  public List<Class<?>> getIgnoredExceptions() {
    return ignoredExceptions.stream()
        .map(name -> {
          try {
            return Class.forName(name);
          } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  public void setFailureRateThreshold(int failureRateThreshold) {
    this.failureRateThreshold = failureRateThreshold;
  }

  @VisibleForTesting
  public void setSlidingWindowSize(int size) {
    this.slidingWindowSize = size;
  }

  @VisibleForTesting
  public void setSlidingWindowMinimumNumberOfCalls(int size) {
    this.slidingWindowMinimumNumberOfCalls = size;
  }

  @VisibleForTesting
  public void setPermittedNumberOfCallsInHalfOpenState(int size) {
    this.permittedNumberOfCallsInHalfOpenState = size;
  }

  @VisibleForTesting
  public void setWaitDurationInOpenState(Duration duration) {
    this.waitDurationInOpenState = duration;
  }

  @VisibleForTesting
  public void setIgnoredExceptions(final List<String> ignoredExceptions) {
    this.ignoredExceptions = ignoredExceptions;
  }

  public CircuitBreakerConfig toCircuitBreakerConfig() {
    return CircuitBreakerConfig.custom()
        .failureRateThreshold(getFailureRateThreshold())
        .ignoreExceptions(getIgnoredExceptions().toArray(new Class[0]))
        .permittedNumberOfCallsInHalfOpenState(getPermittedNumberOfCallsInHalfOpenState())
        .waitDurationInOpenState(getWaitDurationInOpenState())
        .slidingWindow(getSlidingWindowSize(), getSlidingWindowMinimumNumberOfCalls(),
            CircuitBreakerConfig.SlidingWindowType.COUNT_BASED,
            CircuitBreakerConfig.SlidingWindowSynchronizationStrategy.SYNCHRONIZED)
        .build();
  }
}
