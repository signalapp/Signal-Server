/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import javax.validation.constraints.NotEmpty;

public class DynamoDbClientConfiguration {

  private final String region;
  private final Duration clientExecutionTimeout;
  private final Duration clientRequestTimeout;

  @JsonCreator
  public DynamoDbClientConfiguration(
      @JsonProperty("region") final String region,
      @JsonProperty("clientExcecutionTimeout") final Duration clientExecutionTimeout,
      @JsonProperty("clientRequestTimeout") final Duration clientRequestTimeout) {
    this.region = region;
    this.clientExecutionTimeout = clientExecutionTimeout != null ? clientExecutionTimeout : Duration.ofSeconds(30);
    this.clientRequestTimeout = clientRequestTimeout != null ? clientRequestTimeout : Duration.ofSeconds(10);
  }

  @NotEmpty
  public String getRegion() {
    return region;
  }

  public Duration getClientExecutionTimeout() {
    return clientExecutionTimeout;
  }

  public Duration getClientRequestTimeout() {
    return clientRequestTimeout;
  }
}
