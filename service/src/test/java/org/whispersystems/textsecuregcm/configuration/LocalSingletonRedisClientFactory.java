/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.RedisClient;
import io.lettuce.core.resource.ClientResources;
import java.util.concurrent.atomic.AtomicBoolean;
import org.whispersystems.textsecuregcm.redis.RedisSingletonExtension;

@JsonTypeName("local")
public class LocalSingletonRedisClientFactory implements SingletonRedisClientFactory, Managed {

  private static final RedisSingletonExtension redisSingletonExtension = RedisSingletonExtension.builder().build();

  private final AtomicBoolean shutdownHookConfigured = new AtomicBoolean();

  private LocalSingletonRedisClientFactory() {
    try {
      redisSingletonExtension.beforeAll(null);
      redisSingletonExtension.beforeEach(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RedisClient build(final ClientResources clientResources) {

    if (shutdownHookConfigured.compareAndSet(false, true)) {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          this.stop();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));
    }

    return RedisClient.create(clientResources, redisSingletonExtension.getRedisUri());
  }

  @Override
  public void stop() throws Exception {
    redisSingletonExtension.afterEach(null);
    redisSingletonExtension.afterAll(null);
  }
}
