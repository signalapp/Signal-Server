/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;
import io.lettuce.core.RedisClient;
import io.lettuce.core.resource.ClientResources;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = RedisConfiguration.class)
public interface SingletonRedisClientFactory extends Discoverable {

  RedisClient build(ClientResources clientResources);
}
