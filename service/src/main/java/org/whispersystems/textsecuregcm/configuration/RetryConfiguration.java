package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

import java.time.Duration;

import io.github.resilience4j.retry.RetryConfig;

public class RetryConfiguration {

  @JsonProperty
  @Min(1)
  private int maxAttempts = RetryConfig.DEFAULT_MAX_ATTEMPTS;

  @JsonProperty
  @Min(1)
  private long waitDuration = RetryConfig.DEFAULT_WAIT_DURATION;

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public long getWaitDuration() {
    return waitDuration;
  }

  public RetryConfig toRetryConfig() {
    return toRetryConfigBuilder().build();
  }

  public <T> RetryConfig.Builder<T> toRetryConfigBuilder() {
    return RetryConfig.<T>custom()
                      .maxAttempts(getMaxAttempts())
                      .waitDuration(Duration.ofMillis(getWaitDuration()));
  }
}
