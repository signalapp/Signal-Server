/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import jakarta.validation.constraints.Min;
import java.time.Duration;

public class BulkheadConfiguration {

  @JsonProperty
  @Min(1)
  private int maxConcurrentCalls = BulkheadConfig.DEFAULT_MAX_CONCURRENT_CALLS;

  @JsonProperty
  @Min(1)
  private Duration maxWaitDuration = BulkheadConfig.DEFAULT_MAX_WAIT_DURATION;

  public int getMaxConcurrentCalls() {
    return maxConcurrentCalls;
  }

  public void setMaxConcurrentCalls(final int maxConcurrentCalls) {
    this.maxConcurrentCalls = maxConcurrentCalls;
  }

  public Duration getMaxWaitDuration() {
    return maxWaitDuration;
  }

  public void setMaxWaitDuration(final Duration maxWaitDuration) {
    this.maxWaitDuration = maxWaitDuration;
  }

  public BulkheadConfig.Builder toBulkheadConfig() {
    return BulkheadConfig.custom()
        .maxConcurrentCalls(maxConcurrentCalls)
        .maxWaitDuration(maxWaitDuration);
  }
}
