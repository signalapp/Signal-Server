package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

import io.dropwizard.db.DataSourceFactory;

public class DatabaseConfiguration extends DataSourceFactory {

  @NotNull
  @JsonProperty
  private CircuitBreakerConfiguration circuitBreaker = new CircuitBreakerConfiguration();

  public CircuitBreakerConfiguration getCircuitBreakerConfiguration() {
    return circuitBreaker;
  }

}
