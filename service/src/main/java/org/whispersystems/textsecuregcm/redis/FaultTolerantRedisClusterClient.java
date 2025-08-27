/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.configuration.RedisClusterConfiguration;
import reactor.core.scheduler.Schedulers;

/**
 * A fault-tolerant access manager for a Redis cluster. Each shard in the cluster has a dedicated circuit breaker.
 *
 * @see LettuceShardCircuitBreaker
 */
public class FaultTolerantRedisClusterClient {

  private final String name;

  private final RedisClusterClient clusterClient;

  private final StatefulRedisClusterConnection<String, String> stringConnection;
  private final StatefulRedisClusterConnection<byte[], byte[]> binaryConnection;

  private final List<StatefulRedisClusterPubSubConnection<?, ?>> pubSubConnections = new ArrayList<>();

  private final Retry topologyChangedEventRetry;


  public FaultTolerantRedisClusterClient(final String name,
      final RedisClusterConfiguration clusterConfiguration,
      final ClientResources.Builder clientResourcesBuilder) {

    this(name, clientResourcesBuilder,
        Collections.singleton(RedisUriUtil.createRedisUriWithTimeout(clusterConfiguration.getConfigurationUri(),
            clusterConfiguration.getTimeout())),
        clusterConfiguration.getTimeout(),
        clusterConfiguration.getCircuitBreakerConfigurationName());

  }

  FaultTolerantRedisClusterClient(final String name,
      final ClientResources.Builder clientResourcesBuilder,
      final Iterable<RedisURI> redisUris,
      final Duration commandTimeout,
      @Nullable final String circuitBreakerConfigurationName) {

    this.name = name;

    // Lettuce will issue a CLIENT SETINFO command unconditionally if these fields are set (and they are by default),
    // which can generate a bunch of spurious warnings in versions of Redis before 7.2.0.
    //
    // See:
    //
    // - https://github.com/redis/lettuce/pull/2823
    // - https://github.com/redis/lettuce/issues/2817
    redisUris.forEach(redisUri -> {
      redisUri.setClientName(null);
      redisUri.setLibraryName(null);
      redisUri.setLibraryVersion(null);
    });

    final LettuceShardCircuitBreaker lettuceShardCircuitBreaker =
        new LettuceShardCircuitBreaker(name, circuitBreakerConfigurationName);

    this.clusterClient = RedisClusterClient.create(
        clientResourcesBuilder.nettyCustomizer(lettuceShardCircuitBreaker).
            build(),
        redisUris);
    this.clusterClient.setOptions(ClusterClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .validateClusterNodeMembership(false)
        .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
            .enableAllAdaptiveRefreshTriggers()
            .build())
        // for asynchronous commands
        .timeoutOptions(TimeoutOptions.builder()
            .fixedTimeout(commandTimeout)
            .build())
        .publishOnScheduler(true)
        .build());

    this.stringConnection = clusterClient.connect();
    this.binaryConnection = clusterClient.connect(ByteArrayCodec.INSTANCE);

    // create a synthetic topology changed event to notify shard circuit breakers of initial upstreams
    clusterClient.getResources().eventBus().publish(
        new ClusterTopologyChangedEvent(Collections.emptyList(), clusterClient.getPartitions().getPartitions()));

    final RetryConfig topologyChangedEventRetryConfig = RetryConfig.custom()
        .maxAttempts(Integer.MAX_VALUE)
        .intervalFunction(
            IntervalFunction.ofExponentialRandomBackoff(Duration.ofSeconds(1), 1.5, Duration.ofSeconds(30)))
        .build();

    this.topologyChangedEventRetry = Retry.of(name + "-topologyChangedRetry", topologyChangedEventRetryConfig);
  }

  public void shutdown() {
    stringConnection.close();
    binaryConnection.close();

    for (final StatefulRedisClusterPubSubConnection<?, ?> pubSubConnection : pubSubConnections) {
      pubSubConnection.close();
    }

    clusterClient.shutdown();
  }

  public String getName() {
    return name;
  }

  public void useCluster(final Consumer<StatefulRedisClusterConnection<String, String>> consumer) {
    useConnection(stringConnection, consumer);
  }

  public <T> T withCluster(final Function<StatefulRedisClusterConnection<String, String>, T> function) {
    return withConnection(stringConnection, function);
  }

  public void useBinaryCluster(final Consumer<StatefulRedisClusterConnection<byte[], byte[]>> consumer) {
    useConnection(binaryConnection, consumer);
  }

  public <T> T withBinaryCluster(final Function<StatefulRedisClusterConnection<byte[], byte[]>, T> function) {
    return withConnection(binaryConnection, function);
  }

  private <K, V> void useConnection(final StatefulRedisClusterConnection<K, V> connection,
      final Consumer<StatefulRedisClusterConnection<K, V>> consumer) {
    try {
      consumer.accept(connection);
    } catch (final Throwable t) {
      if (t instanceof RedisException) {
        throw (RedisException) t;
      } else {
        throw new RedisException(t);
      }
    }
  }

  private <T, K, V> T withConnection(final StatefulRedisClusterConnection<K, V> connection,
      final Function<StatefulRedisClusterConnection<K, V>, T> function) {
    try {
      return function.apply(connection);
    } catch (final Throwable t) {
      if (t instanceof RedisException) {
        throw (RedisException) t;
      } else {
        throw new RedisException(t);
      }
    }
  }

  public FaultTolerantPubSubClusterConnection<String, String> createPubSubConnection() {
    final StatefulRedisClusterPubSubConnection<String, String> pubSubConnection = clusterClient.connectPubSub();
    pubSubConnections.add(pubSubConnection);

    return new FaultTolerantPubSubClusterConnection<>(name, pubSubConnection, topologyChangedEventRetry,
        Schedulers.newSingle(name + "-redisPubSubEvents", true));
  }

  public FaultTolerantPubSubClusterConnection<byte[], byte[]> createBinaryPubSubConnection() {
    final StatefulRedisClusterPubSubConnection<byte[], byte[]> pubSubConnection = clusterClient.connectPubSub(ByteArrayCodec.INSTANCE);
    pubSubConnections.add(pubSubConnection);

    return new FaultTolerantPubSubClusterConnection<>(name, pubSubConnection, topologyChangedEventRetry,
        Schedulers.newSingle(name + "-redisPubSubEvents", true));
  }
}
