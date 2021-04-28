/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import java.time.Duration;

public class TorExitNodeConfiguration {

  @JsonProperty
  @NotBlank
  private String listUrl;

  @JsonProperty
  private Duration refreshInterval = Duration.ofMinutes(5);

  @JsonProperty
  @Valid
  private CircuitBreakerConfiguration circuitBreakerConfiguration = new CircuitBreakerConfiguration();

  @JsonProperty
  @Valid
  private RetryConfiguration retryConfiguration = new RetryConfiguration();

  public String getListUrl() {
    return listUrl;
  }

  @VisibleForTesting
  public void setListUrl(final String listUrl) {
    this.listUrl = listUrl;
  }

  public Duration getRefreshInterval() {
    return refreshInterval;
  }

  public CircuitBreakerConfiguration getCircuitBreakerConfiguration() {
    return circuitBreakerConfiguration;
  }

  public RetryConfiguration getRetryConfiguration() {
    return retryConfiguration;
  }
}
