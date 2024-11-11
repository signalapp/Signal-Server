/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public class MessageCacheConfiguration {

  @JsonProperty
  @NotNull
  @Valid
  private FaultTolerantRedisClusterFactory cluster;

  @JsonProperty
  private int persistDelayMinutes = 10;

  public FaultTolerantRedisClusterFactory getRedisClusterConfiguration() {
    return cluster;
  }

  public int getPersistDelayMinutes() {
    return persistDelayMinutes;
  }
}
