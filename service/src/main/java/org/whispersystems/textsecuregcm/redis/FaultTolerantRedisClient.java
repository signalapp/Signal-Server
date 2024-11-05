package org.whispersystems.textsecuregcm.redis;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import reactor.core.scheduler.Schedulers;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class FaultTolerantRedisClient {

  private final String name;

  private final RedisClient redisClient;

  private final StatefulRedisConnection<String, String> stringConnection;
  private final StatefulRedisConnection<byte[], byte[]> binaryConnection;

  private final List<StatefulRedisPubSubConnection<?, ?>> pubSubConnections = new ArrayList<>();

  private final CircuitBreaker circuitBreaker;
  private final Retry retry;

  public FaultTolerantRedisClient(final String name,
                                  final RedisConfiguration redisConfiguration,
                                  final ClientResources.Builder clientResourcesBuilder) {

    this(name, clientResourcesBuilder,
        RedisUriUtil.createRedisUriWithTimeout(redisConfiguration.getUri(), redisConfiguration.getTimeout()),
        redisConfiguration.getTimeout(),
        redisConfiguration.getCircuitBreakerConfiguration(),
        redisConfiguration.getRetryConfiguration());
  }

  FaultTolerantRedisClient(String name,
                           final ClientResources.Builder clientResourcesBuilder,
                           final RedisURI redisUri,
                           final Duration commandTimeout,
                           final CircuitBreakerConfiguration circuitBreakerConfiguration,
                           final RetryConfiguration retryConfiguration) {

    this.name = name;

    // Lettuce will issue a CLIENT SETINFO command unconditionally if these fields are set (and they are by default),
    // which can generate a bunch of spurious warnings in versions of Redis before 7.2.0.
    //
    // See:
    //
    // - https://github.com/redis/lettuce/pull/2823
    // - https://github.com/redis/lettuce/issues/2817
    redisUri.setClientName(null);
    redisUri.setLibraryName(null);
    redisUri.setLibraryVersion(null);

    final LettuceShardCircuitBreaker lettuceShardCircuitBreaker = new LettuceShardCircuitBreaker(name,
        circuitBreakerConfiguration.toCircuitBreakerConfig(), Schedulers.newSingle("topology-changed-" + name, true));
    this.redisClient = RedisClient.create(clientResourcesBuilder.build(), redisUri);
    this.redisClient.setOptions(ClusterClientOptions.builder()
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

    lettuceShardCircuitBreaker.setEventBus(redisClient.getResources().eventBus());

    this.stringConnection = redisClient.connect();
    this.binaryConnection = redisClient.connect(ByteArrayCodec.INSTANCE);

    this.circuitBreaker = CircuitBreaker.of(name + "-breaker", circuitBreakerConfiguration.toCircuitBreakerConfig());
    this.retry = Retry.of(name + "-retry", retryConfiguration.toRetryConfigBuilder()
        .retryOnException(exception -> exception instanceof RedisCommandTimeoutException).build());

    CircuitBreakerUtil.registerMetrics(retry, FaultTolerantRedisClusterClient.class);
  }

  public void shutdown() {
    stringConnection.close();

    for (final StatefulRedisPubSubConnection<?, ?> pubSubConnection : pubSubConnections) {
      pubSubConnection.close();
    }

    redisClient.shutdown();
  }

  public String getName() {
    return name;
  }

  public void useConnection(final Consumer<StatefulRedisConnection<String, String>> consumer) {
    useConnection(stringConnection, consumer);
  }

  public <T> T withConnection(final Function<StatefulRedisConnection<String, String>, T> function) {
    return withConnection(stringConnection, function);
  }

  public void useBinaryConnection(final Consumer<StatefulRedisConnection<byte[], byte[]>> consumer) {
    useConnection(binaryConnection, consumer);
  }

  public <T> T withBinaryConnection(final Function<StatefulRedisConnection<byte[], byte[]>, T> function) {
    return withConnection(binaryConnection, function);
  }

  public <K, V> void useConnection(final StatefulRedisConnection<K, V> connection,
      final Consumer<StatefulRedisConnection<K, V>> consumer) {
    try {
      circuitBreaker.executeRunnable(() -> retry.executeRunnable(() -> consumer.accept(connection)));
    } catch (final Throwable t) {
      if (t instanceof RedisException) {
        throw (RedisException) t;
      } else {
        throw new RedisException(t);
      }
    }
  }

  public <T, K, V> T withConnection(final StatefulRedisConnection<K, V> connection,
      final Function<StatefulRedisConnection<K, V>, T> function) {
    try {
      return circuitBreaker.executeCallable(() -> retry.executeCallable(() -> function.apply(connection)));
    } catch (final Throwable t) {
      if (t instanceof RedisException) {
        throw (RedisException) t;
      } else {
        throw new RedisException(t);
      }
    }
  }

  public FaultTolerantPubSubConnection<String, String> createPubSubConnection() {
    final StatefulRedisPubSubConnection<String, String> pubSubConnection = redisClient.connectPubSub();
    pubSubConnections.add(pubSubConnection);

    return new FaultTolerantPubSubConnection<>(name, pubSubConnection);
  }

  public FaultTolerantPubSubConnection<byte[], byte[]> createBinaryPubSubConnection() {
    final StatefulRedisPubSubConnection<byte[], byte[]> pubSubConnection = redisClient.connectPubSub(ByteArrayCodec.INSTANCE);
    pubSubConnections.add(pubSubConnection);

    return new FaultTolerantPubSubConnection<>(name, pubSubConnection);
  }
}
