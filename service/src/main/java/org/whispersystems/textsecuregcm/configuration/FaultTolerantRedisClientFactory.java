/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;
import io.lettuce.core.resource.ClientResources;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = RedisConfiguration.class)
public interface FaultTolerantRedisClientFactory extends Discoverable {

  FaultTolerantRedisClient build(String name, ClientResources clientResources);
}
