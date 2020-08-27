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
