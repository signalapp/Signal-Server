/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Duration;

public class DatadogConfiguration {

  @JsonProperty
  @NotBlank
  private String apiKey;

  @JsonProperty
  @NotNull
  private Duration step = Duration.ofSeconds(10);

  public String getApiKey() {
    return apiKey;
  }

  public Duration getStep() {
    return step;
  }
}
