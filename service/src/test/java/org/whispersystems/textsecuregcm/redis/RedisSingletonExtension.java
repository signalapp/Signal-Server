/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.IOException;
import java.net.ServerSocket;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import redis.embedded.RedisServer;

public class RedisSingletonExtension implements BeforeAllCallback, BeforeEachCallback, AfterAllCallback, AfterEachCallback {

  private static RedisServer redisServer;
  private RedisClient redisClient;

  public static class RedisSingletonExtensionBuilder {

    private RedisSingletonExtensionBuilder() {
    }

    public RedisSingletonExtension build() {
      return new RedisSingletonExtension();
    }
  }

  public static RedisSingletonExtensionBuilder builder() {
    return new RedisSingletonExtensionBuilder();
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

    redisServer.start();
  }

  @Override
  public void beforeEach(final ExtensionContext context) {
    redisClient = RedisClient.create(String.format("redis://127.0.0.1:%d", redisServer.ports().get(0)));

    try (final StatefulRedisConnection<String, String> connection = redisClient.connect()) {
      connection.sync().flushall();
    }
  }

  @Override
  public void afterEach(final ExtensionContext context) {
    redisClient.shutdown();
  }

  @Override
  public void afterAll(final ExtensionContext context) {
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  public RedisClient getRedisClient() {
    return redisClient;
  }

  private static int getAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(false);
      return socket.getLocalPort();
    }
  }
}
