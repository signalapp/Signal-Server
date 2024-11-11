/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.resource.ClientResources;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;

@JsonTypeName("default")
public class RedisConfiguration implements FaultTolerantRedisClientFactory {

  @JsonProperty
  @NotEmpty
  private String uri;

  @JsonProperty
  @NotNull
  private Duration timeout = Duration.ofSeconds(1);

  @JsonProperty
  @NotNull
  @Valid
  private CircuitBreakerConfiguration circuitBreaker = new CircuitBreakerConfiguration();

  @JsonProperty
  @NotNull
  @Valid
  private RetryConfiguration retry = new RetryConfiguration();

  public String getUri() {
    return uri;
  }

  @VisibleForTesting
  public void setUri(String uri) {
    this.uri = uri;
  }

  public Duration getTimeout() {
    return timeout;
  }

  public @NotNull @Valid CircuitBreakerConfiguration getCircuitBreakerConfiguration() {
    return circuitBreaker;
  }

  public @NotNull @Valid RetryConfiguration getRetryConfiguration() {
    return retry;
  }

  @Override
  public FaultTolerantRedisClient build(final String name, final ClientResources clientResources) {
    return new FaultTolerantRedisClient(name, this, clientResources.mutate());
  }
}
