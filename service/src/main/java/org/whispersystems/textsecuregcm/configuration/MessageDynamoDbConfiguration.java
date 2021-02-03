/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.time.Duration;

public class MessageDynamoDbConfiguration {
  private String region;
  private String tableName;
  private Duration timeToLive = Duration.ofDays(7);
  private Duration clientExecutionTimeout = Duration.ofSeconds(30);
  private Duration clientRequestTimeout = Duration.ofSeconds(10);

  @Valid
  @NotEmpty
  public String getRegion() {
    return region;
  }

  @Valid
  @NotEmpty
  public String getTableName() {
    return tableName;
  }

  @Valid
  public Duration getTimeToLive() {
    return timeToLive;
  }

  public Duration getClientExecutionTimeout() {
    return clientExecutionTimeout;
  }

  public Duration getClientRequestTimeout() {
    return clientRequestTimeout;
  }
}
