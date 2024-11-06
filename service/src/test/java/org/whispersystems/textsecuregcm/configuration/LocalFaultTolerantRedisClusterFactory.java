/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.lettuce.core.resource.ClientResources;
import java.util.concurrent.atomic.AtomicBoolean;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;

@JsonTypeName("local")
public class LocalFaultTolerantRedisClusterFactory implements FaultTolerantRedisClusterFactory {

  private static final RedisClusterExtension redisClusterExtension = RedisClusterExtension.builder().build();

  private final AtomicBoolean shutdownHookConfigured = new AtomicBoolean();

  private LocalFaultTolerantRedisClusterFactory() {
    try {
      redisClusterExtension.beforeAll(null);
      redisClusterExtension.beforeEach(null);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FaultTolerantRedisClusterClient build(final String name, final ClientResources.Builder clientResourcesBuilder) {

    if (shutdownHookConfigured.compareAndSet(false, true)) {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          redisClusterExtension.afterEach(null);
          redisClusterExtension.afterAll(null);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));
    }

    final RedisClusterConfiguration config = new RedisClusterConfiguration();
    config.setConfigurationUri(RedisClusterExtension.getRedisURIs().getFirst().toString());

    return new FaultTolerantRedisClusterClient(name, config, clientResourcesBuilder);
  }

}
