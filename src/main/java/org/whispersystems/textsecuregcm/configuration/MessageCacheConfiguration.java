package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.glassfish.jersey.server.JSONP;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class MessageCacheConfiguration {

  @JsonProperty
  @NotNull
  @Valid
  private RedisConfiguration redis;

  @JsonProperty
  private int persistDelayMinutes = 10;

  @JsonProperty
  @Min(0)
  @Max(1)
  private float cacheRate = 1;

  public RedisConfiguration getRedisConfiguration() {
    return redis;
  }

  public int getPersistDelayMinutes() {
    return persistDelayMinutes;
  }

  public float getCacheRate() {
    return cacheRate;
  }
}
