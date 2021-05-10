/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class DonationConfiguration {

  private String uri;
  private String apiKey;
  private Set<String> supportedCurrencies;
  private CircuitBreakerConfiguration circuitBreaker;
  private RetryConfiguration retry;

  @JsonProperty
  @NotEmpty
  public String getUri() {
    return uri;
  }

  @VisibleForTesting
  public void setUri(final String uri) {
    this.uri = uri;
  }

  @JsonProperty
  @NotEmpty
  public String getApiKey() {
    return apiKey;
  }

  @VisibleForTesting
  public void setApiKey(final String apiKey) {
    this.apiKey = apiKey;
  }

  @JsonProperty
  @NotEmpty
  public Set<String> getSupportedCurrencies() {
    return supportedCurrencies;
  }

  @VisibleForTesting
  public void setSupportedCurrencies(final Set<String> supportedCurrencies) {
    this.supportedCurrencies = supportedCurrencies;
  }

  @JsonProperty
  @NotNull
  @Valid
  public CircuitBreakerConfiguration getCircuitBreaker() {
    return circuitBreaker;
  }

  @VisibleForTesting
  public void setCircuitBreaker(final CircuitBreakerConfiguration circuitBreaker) {
    this.circuitBreaker = circuitBreaker;
  }

  @JsonProperty
  @NotNull
  @Valid
  public RetryConfiguration getRetry() {
    return retry;
  }

  @VisibleForTesting
  public void setRetry(final RetryConfiguration retry) {
    this.retry = retry;
  }
}
