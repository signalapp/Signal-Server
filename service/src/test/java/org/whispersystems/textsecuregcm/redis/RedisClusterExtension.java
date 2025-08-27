/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.lettuce.core.FlushMode;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.internal.HostAndPort;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DnsResolvers;
import io.lettuce.core.resource.MappingSocketAddressResolver;
import io.lettuce.core.resource.SocketAddressResolver;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import org.whispersystems.textsecuregcm.util.TestcontainersImages;

public class RedisClusterExtension implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, ExtensionContext.Store.CloseableResource {

  private static ComposeContainer composeContainer;
  private static Map<HostAndPort, HostAndPort> exposedAddressesByInternalAddress;
  private static List<RedisURI> redisUris;

  private final Duration timeout;

  private ClientResources redisClientResources;
  private FaultTolerantRedisClusterClient redisClusterClient;

  private static final String CIRCUIT_BREAKER_CONFIGURATION_NAME =
      RedisClusterExtension.class.getSimpleName() + "-" + RandomStringUtils.insecure().nextAlphanumeric(8);

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(2);

  private static final int REDIS_PORT = 6379;
  private static final WaitStrategy WAIT_STRATEGY = Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(1));
  private static final Duration CLUSTER_UP_DEADLINE = Duration.ofSeconds(15);

  private static final String[] REDIS_SERVICE_NAMES = new String[] { "redis-0-1", "redis-1-1", "redis-2-1" };

  private static final String CLUSTER_COMPOSE_FILE_CONTENTS = String.format("""
      services:
        redis-0:
          image: %1$s
          environment:
            - 'ALLOW_EMPTY_PASSWORD=yes'
            - 'REDIS_NODES=redis-0 redis-1 redis-2'

        redis-1:
          image: %1$s
          environment:
            - 'ALLOW_EMPTY_PASSWORD=yes'
            - 'REDIS_NODES=redis-0 redis-1 redis-2'

        redis-2:
          image: %1$s
          depends_on:
            - redis-0
            - redis-1
          environment:
            - 'ALLOW_EMPTY_PASSWORD=yes'
            - 'REDIS_CLUSTER_REPLICAS=0'
            - 'REDIS_NODES=redis-0 redis-1 redis-2'
            - 'REDIS_CLUSTER_CREATOR=yes'
      """, TestcontainersImages.getRedisCluster());

  public RedisClusterExtension(final Duration timeout) {
    this.timeout = timeout;
  }


  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void close() throws Throwable {
    if (composeContainer != null) {
      composeContainer.stop();
      composeContainer = null;
    }
  }

  @Override
  public void beforeAll(final ExtensionContext context) throws Exception {
    if (composeContainer == null) {
      final CircuitBreakerConfiguration circuitBreakerConfig = new CircuitBreakerConfiguration();
      circuitBreakerConfig.setWaitDurationInOpenState(Duration.ofMillis(500));

      ResilienceUtil.getCircuitBreakerRegistry().addConfiguration(CIRCUIT_BREAKER_CONFIGURATION_NAME, circuitBreakerConfig.toCircuitBreakerConfig());

      final File clusterComposeFile = File.createTempFile("redis-cluster", ".yml");
      clusterComposeFile.deleteOnExit();

      try (final FileOutputStream fileOutputStream = new FileOutputStream(clusterComposeFile)) {
        fileOutputStream.write(CLUSTER_COMPOSE_FILE_CONTENTS.getBytes(StandardCharsets.UTF_8));
      }

      // Unless we specify an explicit list of files to copy to the container, `ComposeContainer` will copy ALL files in
      // the compose file's directory. Please see
      // https://github.com/testcontainers/testcontainers-java/blob/main/docs/modules/docker_compose.md#build-working-directory.
      composeContainer = new ComposeContainer(clusterComposeFile)
          .withCopyFilesInContainer(clusterComposeFile.getName());

      for (final String serviceName : REDIS_SERVICE_NAMES) {
        composeContainer = composeContainer.withExposedService(serviceName, REDIS_PORT, WAIT_STRATEGY);
      }

      composeContainer.start();

      exposedAddressesByInternalAddress = Arrays.stream(REDIS_SERVICE_NAMES)
              .collect(Collectors.toMap(serviceName -> {
                    final String internalIp = composeContainer.getContainerByServiceName(serviceName).orElseThrow()
                        .getContainerInfo()
                        .getNetworkSettings()
                        .getNetworks().values().stream().findFirst().orElseThrow()
                        .getIpAddress();

                    if (internalIp == null) {
                      throw new IllegalStateException("Could not determine internal IP address of service container: " + serviceName);
                    }

                    return HostAndPort.of(internalIp, REDIS_PORT);
                  },
                  serviceName -> HostAndPort.of(
                      composeContainer.getServiceHost(serviceName, REDIS_PORT),
                      composeContainer.getServicePort(serviceName, REDIS_PORT))));

      redisUris = Arrays.stream(REDIS_SERVICE_NAMES)
          .map(serviceName -> RedisURI.create(
              composeContainer.getServiceHost(serviceName, REDIS_PORT),
              composeContainer.getServicePort(serviceName, REDIS_PORT)))
          .toList();

      // Wait for the cluster to be fully up; just having the containers running isn't enough since they still need to do
      // some post-launch cluster setup work.
      boolean allNodesUp;
      final Instant deadline = Instant.now().plus(CLUSTER_UP_DEADLINE);

      final ClientResources clientResources = ClientResources.builder()
          .socketAddressResolver(getSocketAddressResolver())
          .build();

      try {
        do {
          allNodesUp = redisUris.stream()
              .allMatch(redisUri -> {
                try (final RedisClient redisClient = RedisClient.create(clientResources, redisUri)) {
                  final String clusterInfo = redisClient.connect().sync().clusterInfo();
                  return clusterInfo.contains("cluster_state:ok") && clusterInfo.contains("cluster_slots_ok:16384");
                } catch (final Exception e) {
                  return false;
                }
              });

          if (Instant.now().isAfter(deadline)) {
            throw new RuntimeException("Cluster did not start before deadline");
          }

          if (!allNodesUp) {
            Thread.sleep(100);
          }
        } while (!allNodesUp);
      } finally {
        clientResources.shutdown().await();
      }
    }
  }

  @Override
  public void beforeEach(final ExtensionContext context) throws Exception {
    redisClientResources = ClientResources.builder()
        .socketAddressResolver(getSocketAddressResolver())
        .build();

    redisClusterClient = new FaultTolerantRedisClusterClient("test-cluster",
        redisClientResources.mutate(),
        getRedisURIs(),
        timeout,
        CIRCUIT_BREAKER_CONFIGURATION_NAME);

    redisClusterClient.useCluster(connection -> connection.sync().flushall(FlushMode.SYNC));
  }

  @Override
  public void afterEach(final ExtensionContext context) throws InterruptedException {
    redisClusterClient.shutdown();
    redisClientResources.shutdown().await();
  }

  public static List<RedisURI> getRedisURIs() {
    return redisUris;
  }

  public RedisURI getExposedRedisURI(final RedisURI internalRedisURI) {
    final HostAndPort internalHostAndPort = HostAndPort.of(internalRedisURI.getHost(), internalRedisURI.getPort());
    final HostAndPort exposedHostAndPort = exposedAddressesByInternalAddress.getOrDefault(internalHostAndPort, internalHostAndPort);

    return RedisURI.create(exposedHostAndPort.getHostText(), exposedHostAndPort.getPort());
  }

  public SocketAddressResolver getSocketAddressResolver() {
    return MappingSocketAddressResolver.create(DnsResolvers.UNRESOLVED,
        hostAndPort -> exposedAddressesByInternalAddress.getOrDefault(hostAndPort, hostAndPort));
  }

  public FaultTolerantRedisClusterClient getRedisCluster() {
    return redisClusterClient;
  }

  public static class Builder {

    private Duration timeout = DEFAULT_TIMEOUT;

    private Builder() {
    }

    Builder timeout(Duration timeout) {
      this.timeout = timeout;
      return this;
    }

    public RedisClusterExtension build() {
      return new RedisClusterExtension(timeout);
    }
  }
}
