/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class MessageCacheConfiguration {

  @JsonProperty
  @NotNull
  @Valid
  private RedisClusterConfiguration cluster;

  @JsonProperty
  private int persistDelayMinutes = 10;

  public RedisClusterConfiguration getRedisClusterConfiguration() {
    return cluster;
  }

  public int getPersistDelayMinutes() {
    return persistDelayMinutes;
  }

}
