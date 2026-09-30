/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.MaintNotificationsConfig;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.RedisClusterConfiguration;
import reactor.core.scheduler.Schedulers;

/**
 * A fault-tolerant access manager for a Redis cluster. Each shard in the cluster has a dedicated circuit breaker.
 *
 * @see LettuceShardCircuitBreaker
 */
public class FaultTolerantRedisClusterClient {

  private final String name;

  private final Duration upstreamConnectionTimeout;

  private final RedisClusterClient clusterClient;

  private final StatefulRedisClusterConnection<String, String> stringConnection;
  private final StatefulRedisClusterConnection<byte[], byte[]> binaryConnection;

  private final List<StatefulRedisClusterPubSubConnection<?, ?>> pubSubConnections = new ArrayList<>();

  private final Retry topologyChangedEventRetry;

  private static final Logger logger = LoggerFactory.getLogger(FaultTolerantRedisClusterClient.class);


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
    this.upstreamConnectionTimeout = commandTimeout;

    final LettuceShardCircuitBreaker lettuceShardCircuitBreaker =
        new LettuceShardCircuitBreaker(name, circuitBreakerConfigurationName);

    this.clusterClient = RedisClusterClient.create(
        clientResourcesBuilder.nettyCustomizer(lettuceShardCircuitBreaker)
            .build(),
        redisUris);

    final ClusterClientOptions.Builder clusterClientOptionsBuilder = (ClusterClientOptions.Builder) ClusterClientOptions.builder()
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
        .maintNotificationsConfig(MaintNotificationsConfig.disabled());

    NettyUtil.setSocketTimeoutsIfApplicable(clusterClientOptionsBuilder);

    this.clusterClient.setOptions(clusterClientOptionsBuilder.build());

    this.stringConnection = clusterClient.connect();
    this.binaryConnection = clusterClient.connect(ByteArrayCodec.INSTANCE);

    // Eagerly initialize connections. Otherwise, the first several calls will fail immediately, rather than being queued
    // while the connection is pending. See https://github.com/redis/lettuce/pull/3782.
    awaitUpstreamConnections(
        connectToAllUpstreams(stringConnection.getPartitions(), stringConnection::getConnectionAsync),
        connectToAllUpstreams(binaryConnection.getPartitions(), binaryConnection::getConnectionAsync));

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

    awaitUpstreamConnections(
        connectToAllUpstreams(pubSubConnection.getPartitions(), pubSubConnection::getConnectionAsync));

    return new FaultTolerantPubSubClusterConnection<>(name, pubSubConnection, topologyChangedEventRetry,
        Schedulers.newSingle(name + "-redisPubSubEvents", true));
  }

  public FaultTolerantPubSubClusterConnection<byte[], byte[]> createBinaryPubSubConnection() {
    final StatefulRedisClusterPubSubConnection<byte[], byte[]> pubSubConnection = clusterClient.connectPubSub(ByteArrayCodec.INSTANCE);
    pubSubConnections.add(pubSubConnection);

    awaitUpstreamConnections(
        connectToAllUpstreams(pubSubConnection.getPartitions(), pubSubConnection::getConnectionAsync));

    return new FaultTolerantPubSubClusterConnection<>(name, pubSubConnection, topologyChangedEventRetry,
        Schedulers.newSingle(name + "-redisPubSubEvents", true));
  }

  /// Initiates connections to all upstream nodes in the cluster
  ///
  /// @return a future that completes when all connection attempts have either succeeded or failed. This future will never fail.
  private CompletableFuture<Void> connectToAllUpstreams(final Partitions partitions,
      final BiFunction<String, Integer, CompletableFuture<?>> connectionFunction) {

    return CompletableFuture.allOf(partitions.stream()
        .filter(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM))
        .map(node -> connectionFunction.apply(node.getUri().getHost(), node.getUri().getPort())
            .exceptionally(throwable -> {
              logger.warn("Failed to connect to upstream {} for {}", node.getUri(), name, throwable);
              return null;
            }))
        .toArray(CompletableFuture[]::new));
  }

  /// Awaits the futures until {@link FaultTolerantRedisClusterClient#upstreamConnectionTimeout} is reached
  void awaitUpstreamConnections(final CompletableFuture<?>... connectionFutures) {
    try {
      CompletableFuture.allOf(connectionFutures).get(upstreamConnectionTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (final TimeoutException | InterruptedException e) {
      logger.warn("Upstream connection to {} timed out or interrupted; continuing", this.name, e);
    } catch (final ExecutionException e) {
      throw new IllegalStateException("Unexpected: connection attempt futures always complete successfully", e);
    }
  }

}
