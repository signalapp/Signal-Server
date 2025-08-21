/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micrometer.registry.otlp.HistogramFlavor;
import io.micrometer.registry.otlp.OtlpConfig;
import java.time.Duration;
import java.util.Map;

public record OpenTelemetryConfiguration(
  @JsonProperty boolean enabled,
  @JsonProperty Duration shutdownWaitDuration,
  @JsonProperty int maxBucketCount,
  @JsonProperty Map<String, Integer> maxBucketsPerMeter,
  @JsonAnyGetter @JsonAnySetter Map<String, String> otlpConfig
) implements OtlpConfig {

  @Override
  public String get(String key) {
    return otlpConfig.get(key.split("\\.", 2)[1]);
  }

  @Override
  public Map<String, Integer> maxBucketsPerMeter() {
    if (maxBucketsPerMeter == null) {
      return Map.of();
    }
    return maxBucketsPerMeter;
  }

  @Override
  public HistogramFlavor histogramFlavor() {
    return HistogramFlavor.BASE2_EXPONENTIAL_BUCKET_HISTOGRAM;
  }

  public Duration shutdownWaitDuration() {
    if (shutdownWaitDuration == null) {
      return step().plus(step().dividedBy(2));
    }
    return shutdownWaitDuration;
  }

}
