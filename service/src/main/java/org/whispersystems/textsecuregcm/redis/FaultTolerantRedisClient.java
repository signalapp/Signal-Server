package org.whispersystems.textsecuregcm.redis;

import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.whispersystems.textsecuregcm.configuration.RedisConfiguration;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;

public class FaultTolerantRedisClient {

  private final String name;

  private final RedisClient redisClient;

  private final StatefulRedisConnection<String, String> stringConnection;
  private final StatefulRedisConnection<byte[], byte[]> binaryConnection;

  private final List<StatefulRedisPubSubConnection<?, ?>> pubSubConnections = new ArrayList<>();

  private final CircuitBreaker circuitBreaker;

  public FaultTolerantRedisClient(final String name,
                                  final RedisConfiguration redisConfiguration,
                                  final ClientResources.Builder clientResourcesBuilder) {

    this(name, clientResourcesBuilder,
        RedisUriUtil.createRedisUriWithTimeout(redisConfiguration.getUri(), redisConfiguration.getTimeout()),
        redisConfiguration.getTimeout(),
        redisConfiguration.getCircuitBreakerConfigurationName() != null
            ? ResilienceUtil.getCircuitBreakerRegistry().circuitBreaker(getCircuitBreakerName(name), redisConfiguration.getCircuitBreakerConfigurationName())
            : ResilienceUtil.getCircuitBreakerRegistry().circuitBreaker(getCircuitBreakerName(name)));
  }

  private static String getCircuitBreakerName(final String name) {
    return ResilienceUtil.name(FaultTolerantRedisClient.class, name);
  }

  @VisibleForTesting
  FaultTolerantRedisClient(String name,
                           final ClientResources.Builder clientResourcesBuilder,
                           final RedisURI redisUri,
                           final Duration commandTimeout,
                           final CircuitBreaker circuitBreaker) {

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

    this.redisClient = RedisClient.create(clientResourcesBuilder.build(), redisUri);
    this.redisClient.setOptions(ClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        // for asynchronous commands
        .timeoutOptions(TimeoutOptions.builder()
            .fixedTimeout(commandTimeout)
            .build())
        .publishOnScheduler(true)
        .build());

    this.stringConnection = redisClient.connect();
    this.binaryConnection = redisClient.connect(ByteArrayCodec.INSTANCE);

    this.circuitBreaker = circuitBreaker;
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

  private <K, V> void useConnection(final StatefulRedisConnection<K, V> connection,
      final Consumer<StatefulRedisConnection<K, V>> consumer) {
    try {
      circuitBreaker.executeRunnable(() -> consumer.accept(connection));
    } catch (final Throwable t) {
      if (t instanceof RedisException) {
        throw (RedisException) t;
      } else {
        throw new RedisException(t);
      }
    }
  }

  private <T, K, V> T withConnection(final StatefulRedisConnection<K, V> connection,
      final Function<StatefulRedisConnection<K, V>, T> function) {
    try {
      return circuitBreaker.executeCallable(() -> function.apply(connection));
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
