/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.resource.ClientResources;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.util.RedisClusterUtil;
import redis.embedded.RedisServer;
import redis.embedded.exceptions.EmbeddedRedisException;

public class RedisClusterExtension implements BeforeAllCallback, BeforeEachCallback, AfterAllCallback,
    AfterEachCallback {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(2);
  private static final int NODE_COUNT = 2;

  private static final RedisServer[] CLUSTER_NODES = new RedisServer[NODE_COUNT];

  private final Duration timeout;
  private final RetryConfiguration retryConfiguration;
  private FaultTolerantRedisClusterClient redisCluster;
  private ClientResources redisClientResources;

  public RedisClusterExtension(final Duration timeout, final RetryConfiguration retryConfiguration) {
    this.timeout = timeout;
    this.retryConfiguration = retryConfiguration;
  }


  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void afterAll(final ExtensionContext context) throws Exception {
    for (final RedisServer node : CLUSTER_NODES) {
      node.stop();
    }
  }

  @Override
  public void afterEach(final ExtensionContext context) throws Exception {
    redisCluster.shutdown();
    redisClientResources.shutdown().get();
  }

  @Override
  public void beforeAll(final ExtensionContext context) throws Exception {
    assumeFalse(System.getProperty("os.name").equalsIgnoreCase("windows"));

    for (int i = 0; i < NODE_COUNT; i++) {
      // We're occasionally seeing redis server startup failing due to the bind address being already in use.
      // To mitigate that, we're going to just retry a couple of times before failing the test.
      CLUSTER_NODES[i] = startWithRetries(3);
    }

    assembleCluster(CLUSTER_NODES);
  }

  @Override
  public void beforeEach(final ExtensionContext context) throws Exception {

    redisClientResources = ClientResources.builder().build();
    final CircuitBreakerConfiguration circuitBreakerConfig = new CircuitBreakerConfiguration();
    circuitBreakerConfig.setWaitDurationInOpenState(Duration.ofMillis(500));
    redisCluster = new FaultTolerantRedisClusterClient("test-cluster",
        redisClientResources.mutate(),
        getRedisURIs(),
        timeout,
        circuitBreakerConfig,
        retryConfiguration);

    redisCluster.useCluster(connection -> {
      boolean setAll = false;

      final String[] keys = new String[NODE_COUNT];

      for (int i = 0; i < keys.length; i++) {
        keys[i] = RedisClusterUtil.getMinimalHashTag(i * SlotHash.SLOT_COUNT / keys.length);
      }

      while (!setAll) {
        try {
          for (final String key : keys) {
            connection.sync().set(key, "warmup");
          }

          setAll = true;
        } catch (final RedisException | CallNotPermittedException ignored) {
          // Cluster isn't ready; wait and retry.
          try {
            Thread.sleep(500);
          } catch (final InterruptedException ignored2) {
          }
        }
      }
    });

    redisCluster.useCluster(connection -> connection.sync().flushall());
  }

  public static List<RedisURI> getRedisURIs() {
    return Arrays.stream(CLUSTER_NODES)
        .map(node -> "redis://127.0.0.1:%d".formatted(node.ports().getFirst()))
        .map(RedisURI::create)
        .toList();
  }

  public FaultTolerantRedisClusterClient getRedisCluster() {
    return redisCluster;
  }

  private static RedisServer buildClusterNode(final int port) throws IOException {
    final File clusterConfigFile = File.createTempFile("redis", ".conf");
    clusterConfigFile.deleteOnExit();

    return RedisServer.builder()
        .setting("cluster-enabled yes")
        .setting("cluster-config-file " + clusterConfigFile.getAbsolutePath())
        .setting("cluster-node-timeout 5000")
        .setting("appendonly no")
        .setting("save \"\"")
        .setting("dir " + System.getProperty("java.io.tmpdir"))
        .port(port)
        .build();
  }

  private static void assembleCluster(final RedisServer... nodes) throws InterruptedException {
    try (final RedisClient meetClient = RedisClient.create(RedisURI.create("127.0.0.1", nodes[0].ports().getFirst()))) {
      final StatefulRedisConnection<String, String> connection = meetClient.connect();
      final RedisCommands<String, String> commands = connection.sync();

      for (int i = 1; i < nodes.length; i++) {
        commands.clusterMeet("127.0.0.1", nodes[i].ports().getFirst());
      }
    }

    final int slotsPerNode = SlotHash.SLOT_COUNT / nodes.length;

    for (int i = 0; i < nodes.length; i++) {
      final int startInclusive = i * slotsPerNode;
      final int endExclusive = i == nodes.length - 1 ? SlotHash.SLOT_COUNT : (i + 1) * slotsPerNode;

      try (final RedisClient assignSlotClient = RedisClient.create(
          RedisURI.create("127.0.0.1", nodes[i].ports().getFirst()));
          final StatefulRedisConnection<String, String> assignSlotConnection = assignSlotClient.connect()) {
        final int[] slots = new int[endExclusive - startInclusive];

        for (int s = startInclusive; s < endExclusive; s++) {
          slots[s - startInclusive] = s;
        }

        assignSlotConnection.sync().clusterAddSlots(slots);
      }
    }

    try (final RedisClient waitClient = RedisClient.create(RedisURI.create("127.0.0.1", nodes[0].ports().getFirst()));
        final StatefulRedisConnection<String, String> connection = waitClient.connect()) {
      // CLUSTER INFO gives us a big blob of key-value pairs, but the one we're interested in is `cluster_state`.
      // According to https://redis.io/commands/cluster-info, `cluster_state:ok` means that the node is ready to
      // receive queries, all slots are assigned, and a majority of leader nodes are reachable.

      final int sleepMillis = 500;
      int tries = 0;
      while (!connection.sync().clusterInfo().contains("cluster_state:ok")) {
        Thread.sleep(sleepMillis);
        tries++;

        if (tries == 20) {
          throw new RuntimeException(
              "Timeout: Redis not ready after waiting %d milliseconds".formatted(tries * sleepMillis));
        }
      }
    }
  }

  public static int getNextRedisClusterPort() throws IOException {
    final int maxIterations = 11_000;
    for (int i = 0; i < maxIterations; i++) {
      try (final ServerSocket socket = new ServerSocket(0)) {
        final int port = socket.getLocalPort();
        if (port < 55535) {
          return port;
        }
      }
    }
    throw new IOException("Couldn't find an unused open port below 55,535 in " + maxIterations + " tries");
  }

  private static RedisServer startWithRetries(final int attemptsLeft) throws Exception {
    try {
      final RedisServer redisServer = buildClusterNode(getNextRedisClusterPort());
      redisServer.start();
      return redisServer;
    } catch (final IOException | EmbeddedRedisException e) {
      if (attemptsLeft == 0) {
        throw e;
      }
      Thread.sleep(500);
      return startWithRetries(attemptsLeft - 1);
    }
  }

  public static class Builder {

    private Duration timeout = DEFAULT_TIMEOUT;
    private RetryConfiguration retryConfiguration = new RetryConfiguration();

    private Builder() {
    }

    Builder timeout(Duration timeout) {
      this.timeout = timeout;
      return this;
    }

    Builder retryConfiguration(RetryConfiguration retryConfiguration) {
      this.retryConfiguration = retryConfiguration;
      return this;
    }

    public RedisClusterExtension build() {
      return new RedisClusterExtension(timeout, retryConfiguration);
    }
  }
}
