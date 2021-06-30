/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micrometer.datadog.DatadogConfig;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Duration;

public class DatadogConfiguration implements DatadogConfig {

  @JsonProperty
  @NotBlank
  private String apiKey;

  @JsonProperty
  @NotNull
  private Duration step = Duration.ofSeconds(10);

  @JsonProperty
  @NotBlank
  private String environment;

  @JsonProperty
  @Min(1)
  private int batchSize = 5_000;

  @Override
  public String apiKey() {
    return apiKey;
  }

  @Override
  public Duration step() {
    return step;
  }

  public String getEnvironment() {
    return environment;
  }

  @Override
  public int batchSize() {
    return batchSize;
  }

  @Override
  public String hostTag() {
    return "host";
  }

  @Override
  public String get(final String key) {
    return null;
  }
}
