package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

/**
 * Configuration used to interact with a cdn via HTTP
 */
public class ClientCdnConfiguration {

  @JsonProperty
  @NotNull
  @Valid
  CircuitBreakerConfiguration circuitBreaker = new CircuitBreakerConfiguration();

  @JsonProperty
  @NotNull
  @Valid
  RetryConfiguration retry = new RetryConfiguration();

  public CircuitBreakerConfiguration getCircuitBreaker() {
    return circuitBreaker;
  }

  public RetryConfiguration getRetry() {
    return retry;
  }
}
