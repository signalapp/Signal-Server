/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.lettuce.core.RedisClient;
import io.lettuce.core.resource.ClientResources;
import java.time.Duration;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.redis.RedisUriUtil;

@JsonTypeName("default")
public class RedisConfiguration implements SingletonRedisClientFactory {

  @JsonProperty
  @NotEmpty
  private String uri;

  @JsonProperty
  @NotNull
  private Duration timeout = Duration.ofSeconds(1);

  public String getUri() {
    return uri;
  }

  public Duration getTimeout() {
    return timeout;
  }

  @Override
  public RedisClient build(final ClientResources clientResources) {
    final RedisClient redisClient = RedisClient.create(clientResources,
        RedisUriUtil.createRedisUriWithTimeout(uri, timeout));
    redisClient.setDefaultTimeout(timeout);

    return redisClient;
  }
}
