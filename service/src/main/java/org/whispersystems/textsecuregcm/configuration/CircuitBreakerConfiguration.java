/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class CircuitBreakerConfiguration {

  @JsonProperty
  @NotNull
  @Min(1)
  @Max(100)
  private int failureRateThreshold = 50;

  @JsonProperty
  @NotNull
  @Min(1)
  private int ringBufferSizeInHalfOpenState = 10;

  @JsonProperty
  @NotNull
  @Min(1)
  private int ringBufferSizeInClosedState = 100;

  @JsonProperty
  @NotNull
  @Min(1)
  private long waitDurationInOpenStateInSeconds = 10;

  @JsonProperty
  private List<String> ignoredExceptions = Collections.emptyList();


  public int getFailureRateThreshold() {
    return failureRateThreshold;
  }

  public int getRingBufferSizeInHalfOpenState() {
    return ringBufferSizeInHalfOpenState;
  }

  public int getRingBufferSizeInClosedState() {
    return ringBufferSizeInClosedState;
  }

  public long getWaitDurationInOpenStateInSeconds() {
    return waitDurationInOpenStateInSeconds;
  }

  public List<Class> getIgnoredExceptions() {
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
  public void setRingBufferSizeInClosedState(int size) {
    this.ringBufferSizeInClosedState = size;
  }

  @VisibleForTesting
  public void setRingBufferSizeInHalfOpenState(int size) {
    this.ringBufferSizeInHalfOpenState = size;
  }

  @VisibleForTesting
  public void setWaitDurationInOpenStateInSeconds(int seconds) {
    this.waitDurationInOpenStateInSeconds = seconds;
  }

  @VisibleForTesting
  public void setIgnoredExceptions(final List<String> ignoredExceptions) {
    this.ignoredExceptions = ignoredExceptions;
  }

  public CircuitBreakerConfig toCircuitBreakerConfig() {
    return CircuitBreakerConfig.custom()
                        .failureRateThreshold(getFailureRateThreshold())
                        .ignoreExceptions(getIgnoredExceptions().toArray(new Class[0]))
                        .ringBufferSizeInHalfOpenState(getRingBufferSizeInHalfOpenState())
                        .waitDurationInOpenState(Duration.ofSeconds(getWaitDurationInOpenStateInSeconds()))
                        .ringBufferSizeInClosedState(getRingBufferSizeInClosedState())
                        .build();
  }
}
