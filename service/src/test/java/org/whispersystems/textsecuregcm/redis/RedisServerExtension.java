/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import com.redis.testcontainers.RedisContainer;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.lettuce.core.FlushMode;
import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;
import java.time.Duration;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.utility.DockerImageName;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.TestcontainersImages;

public class RedisServerExtension implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, ExtensionContext.Store.CloseableResource {

  private static RedisContainer redisContainer;

  private ClientResources redisClientResources;
  private FaultTolerantRedisClient faultTolerantRedisClient;

  private static final DockerImageName REDIS_IMAGE = DockerImageName.parse(TestcontainersImages.getRedis());

  public static class RedisServerExtensionBuilder {

    private RedisServerExtensionBuilder() {
    }

    public RedisServerExtension build() {
      return new RedisServerExtension();
    }
  }

  public static RedisServerExtensionBuilder builder() {
    return new RedisServerExtensionBuilder();
  }

  @Override
  public void beforeAll(final ExtensionContext context) {
    if (redisContainer == null) {
      redisContainer = new RedisContainer(REDIS_IMAGE);
      redisContainer.start();
    }
  }

  public static RedisURI getRedisURI() {
    return RedisURI.create(redisContainer.getRedisURI());
  }

  @Override
  public void beforeEach(final ExtensionContext context) {
    final CircuitBreakerConfiguration circuitBreakerConfig = new CircuitBreakerConfiguration();
    circuitBreakerConfig.setWaitDurationInOpenState(Duration.ofMillis(500));

    redisClientResources = ClientResources.builder().build();

    faultTolerantRedisClient = new FaultTolerantRedisClient("test-redis-client",
        redisClientResources.mutate(),
        getRedisURI(),
        Duration.ofSeconds(2),
        CircuitBreaker.of("test", circuitBreakerConfig.toCircuitBreakerConfig()));

    faultTolerantRedisClient.useConnection(connection -> connection.sync().flushall(FlushMode.SYNC));
  }

  @Override
  public void afterEach(final ExtensionContext context) throws InterruptedException {
    faultTolerantRedisClient.shutdown();
    redisClientResources.shutdown().await();
  }

  @Override
  public void close() throws Throwable {
    if (redisContainer != null) {
      redisContainer.stop();
      redisContainer = null;
    }
  }

  public FaultTolerantRedisClient getRedisClient() {
    return faultTolerantRedisClient;
  }
}
