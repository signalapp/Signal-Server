/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

import io.lettuce.core.RedisURI;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import io.lettuce.core.resource.ClientResources;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import redis.embedded.RedisServer;
import redis.embedded.exceptions.EmbeddedRedisException;

public class RedisServerExtension implements BeforeAllCallback, BeforeEachCallback, AfterAllCallback, AfterEachCallback {

  private static RedisServer redisServer;
  private FaultTolerantRedisClient faultTolerantRedisClient;
  private ClientResources redisClientResources;

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
  public void beforeAll(final ExtensionContext context) throws Exception {
    assumeFalse(System.getProperty("os.name").equalsIgnoreCase("windows"));

    redisServer = RedisServer.builder()
        .setting("appendonly no")
        .setting("save \"\"")
        .setting("dir " + System.getProperty("java.io.tmpdir"))
        .port(getAvailablePort())
        .build();

    startWithRetries(3);
  }

  public static RedisURI getRedisURI() {
    return RedisURI.create("redis://127.0.0.1:%d".formatted(redisServer.ports().getFirst()));
  }

  @Override
  public void beforeEach(final ExtensionContext context) {
    redisClientResources = ClientResources.builder().build();
    final CircuitBreakerConfiguration circuitBreakerConfig = new CircuitBreakerConfiguration();
    circuitBreakerConfig.setWaitDurationInOpenState(Duration.ofMillis(500));
    faultTolerantRedisClient = new FaultTolerantRedisClient("test-redis-client",
        redisClientResources.mutate(),
        getRedisURI(),
        Duration.ofSeconds(2),
        circuitBreakerConfig,
        new RetryConfiguration());

    faultTolerantRedisClient.useConnection(connection -> connection.sync().flushall());
  }

  @Override
  public void afterEach(final ExtensionContext context) throws InterruptedException {
    redisClientResources.shutdown().await();
  }

  @Override
  public void afterAll(final ExtensionContext context) {
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  public FaultTolerantRedisClient getRedisClient() {
    return faultTolerantRedisClient;
  }

  private static int getAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private void startWithRetries(int attemptsLeft) throws Exception {
    try {
      redisServer.start();
    } catch (final EmbeddedRedisException e) {
      if (attemptsLeft == 0) {
        throw e;
      }
      Thread.sleep(500);
      startWithRetries(attemptsLeft - 1);
    }
  }
}
