package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.time.Duration;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;

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

  public CircuitBreakerConfig toCircuitBreakerConfig() {
    return CircuitBreakerConfig.custom()
                        .failureRateThreshold(getFailureRateThreshold())
                        .ringBufferSizeInHalfOpenState(getRingBufferSizeInHalfOpenState())
                        .waitDurationInOpenState(Duration.ofSeconds(getWaitDurationInOpenStateInSeconds()))
                        .ringBufferSizeInClosedState(getRingBufferSizeInClosedState())
                        .build();
  }
}
