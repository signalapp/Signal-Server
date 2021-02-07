/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;


import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class RedisConfiguration {

  @JsonProperty
  @NotEmpty
  private String url;

  @JsonProperty
  @NotNull
  private List<String> replicaUrls;

  @JsonProperty
  @NotNull
  private Duration timeout = Duration.ofSeconds(10);

  @JsonProperty
  @NotNull
  @Valid
  private CircuitBreakerConfiguration circuitBreaker = new CircuitBreakerConfiguration();

  public String getUrl() {
    return url;
  }

  public List<String> getReplicaUrls() {
    return replicaUrls;
  }

  public Duration getTimeout() {
    return timeout;
  }

  public CircuitBreakerConfiguration getCircuitBreakerConfiguration() {
    return circuitBreaker;
  }
}
