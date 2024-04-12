/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.reactor.retry.RetryOperator;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisClusterConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * A fault-tolerant access manager for a Redis cluster. Each shard in the cluster has a dedicated circuit breaker.
 *
 * @see LettuceShardCircuitBreaker
 */
public class FaultTolerantRedisCluster {

  private final String name;

  private final RedisClusterClient clusterClient;

  private final StatefulRedisClusterConnection<String, String> stringConnection;
  private final StatefulRedisClusterConnection<byte[], byte[]> binaryConnection;

  private final List<StatefulRedisClusterPubSubConnection<?, ?>> pubSubConnections = new ArrayList<>();

  private final Retry retry;
  private final Retry topologyChangedEventRetry;


  public FaultTolerantRedisCluster(final String name, final RedisClusterConfiguration clusterConfiguration,
      final ClientResources.Builder clientResourcesBuilder) {

    this(name, clientResourcesBuilder,
        Collections.singleton(RedisUriUtil.createRedisUriWithTimeout(clusterConfiguration.getConfigurationUri(),
            clusterConfiguration.getTimeout())),
        clusterConfiguration.getTimeout(),
        clusterConfiguration.getCircuitBreakerConfiguration(),
        clusterConfiguration.getRetryConfiguration());

  }

  FaultTolerantRedisCluster(String name, final ClientResources.Builder clientResourcesBuilder,
      Iterable<RedisURI> redisUris, Duration commandTimeout, CircuitBreakerConfiguration circuitBreakerConfig,
      RetryConfiguration retryConfiguration) {

    this.name = name;

    this.clusterClient = RedisClusterClient.create(
        clientResourcesBuilder.nettyCustomizer(
                new LettuceShardCircuitBreaker(name, circuitBreakerConfig.toCircuitBreakerConfig())).
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

    this.retry = Retry.of(name + "-retry", retryConfiguration.toRetryConfigBuilder()
        .retryOnException(exception -> exception instanceof RedisCommandTimeoutException).build());
    final RetryConfig topologyChangedEventRetryConfig = RetryConfig.custom()
        .maxAttempts(Integer.MAX_VALUE)
        .intervalFunction(
            IntervalFunction.ofExponentialRandomBackoff(Duration.ofSeconds(1), 1.5, Duration.ofSeconds(30)))
        .build();

    this.topologyChangedEventRetry = Retry.of(name + "-topologyChangedRetry", topologyChangedEventRetryConfig);

    CircuitBreakerUtil.registerMetrics(retry, FaultTolerantRedisCluster.class);
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

  public <T> Publisher<T> withBinaryClusterReactive(
      final Function<StatefulRedisClusterConnection<byte[], byte[]>, Publisher<T>> function) {
    return withConnectionReactive(binaryConnection, function);
  }

  public <K, V> void useConnection(final StatefulRedisClusterConnection<K, V> connection,
      final Consumer<StatefulRedisClusterConnection<K, V>> consumer) {
    try {
      retry.executeRunnable(() -> consumer.accept(connection));
    } catch (final Throwable t) {
      if (t instanceof RedisException) {
        throw (RedisException) t;
      } else {
        throw new RedisException(t);
      }
    }
  }

  public <T, K, V> T withConnection(final StatefulRedisClusterConnection<K, V> connection,
      final Function<StatefulRedisClusterConnection<K, V>, T> function) {
    try {
      return retry.executeCallable(() -> function.apply(connection));
    } catch (final Throwable t) {
      if (t instanceof RedisException) {
        throw (RedisException) t;
      } else {
        throw new RedisException(t);
      }
    }
  }

  public <T, K, V> Publisher<T> withConnectionReactive(final StatefulRedisClusterConnection<K, V> connection,
      final Function<StatefulRedisClusterConnection<K, V>, Publisher<T>> function) {

    return Flux.from(function.apply(connection))
        .transformDeferred(RetryOperator.of(retry));
  }

  public FaultTolerantPubSubConnection<String, String> createPubSubConnection() {
    final StatefulRedisClusterPubSubConnection<String, String> pubSubConnection = clusterClient.connectPubSub();
    pubSubConnections.add(pubSubConnection);

    return new FaultTolerantPubSubConnection<>(name, pubSubConnection, retry, topologyChangedEventRetry,
        Schedulers.newSingle(name + "-redisPubSubEvents", true));
  }

}
