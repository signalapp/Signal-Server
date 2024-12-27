/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.models.partitions.ClusterPartitionParser;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.event.EventBus;
import io.lettuce.core.event.EventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyCollectorOptions;
import io.lettuce.core.metrics.CommandLatencyRecorder;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.Delay;
import io.lettuce.core.resource.DnsResolver;
import io.lettuce.core.resource.EventLoopGroupProvider;
import io.lettuce.core.resource.NettyCustomizer;
import io.lettuce.core.resource.SocketAddressResolver;
import io.lettuce.core.resource.ThreadFactoryProvider;
import io.lettuce.core.tracing.Tracing;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.Timer;
import io.netty.util.concurrent.EventExecutorGroup;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.RedisClusterUtil;

// ThreadMode.SEPARATE_THREAD protects against hangs in the remote Redis calls, as this mode allows the test code to be
// preempted by the timeout check
@Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FaultTolerantRedisClusterClientTest {

  private static final Duration TIMEOUT = Duration.ofMillis(50);

  private static final RetryConfiguration RETRY_CONFIGURATION = new RetryConfiguration();

  static {
    RETRY_CONFIGURATION.setMaxAttempts(1);
    RETRY_CONFIGURATION.setWaitDuration(50);
  }

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder()
      .retryConfiguration(RETRY_CONFIGURATION)
      .timeout(TIMEOUT)
      .build();

  private FaultTolerantRedisClusterClient cluster;

  private static FaultTolerantRedisClusterClient buildCluster(
      @Nullable final CircuitBreakerConfiguration circuitBreakerConfiguration,
      final ClientResources.Builder clientResourcesBuilder) {

    return new FaultTolerantRedisClusterClient("test", clientResourcesBuilder,
        RedisClusterExtension.getRedisURIs(), TIMEOUT,
        Optional.ofNullable(circuitBreakerConfiguration).orElseGet(CircuitBreakerConfiguration::new),
        RETRY_CONFIGURATION);
  }

  @AfterEach
  void tearDown() {
    cluster.shutdown();
  }

  @Test
  void testTimeout() {
    cluster = buildCluster(null, ClientResources.builder());

    final ExecutionException asyncException = assertThrows(ExecutionException.class,
        () -> cluster.withCluster(connection -> connection.async().blpop(10 * TIMEOUT.toMillis() / 1000d, "key"))
            .get());

    assertInstanceOf(RedisCommandTimeoutException.class, asyncException.getCause());

    assertThrows(RedisCommandTimeoutException.class,
        () -> cluster.withCluster(connection -> connection.sync().blpop(10 * TIMEOUT.toMillis() / 1000d, "key")));
  }

  @Test
  void testTimeoutCircuitBreaker() throws Exception {
    // because we’re using a single key, and blpop involves *Redis* also blocking, the breaker wait duration must be
    // longer than the sum of the remote timeouts
    final Duration breakerWaitDuration = TIMEOUT.multipliedBy(5);

    final CircuitBreakerConfiguration circuitBreakerConfig = new CircuitBreakerConfiguration();
    circuitBreakerConfig.setFailureRateThreshold(1);
    circuitBreakerConfig.setSlidingWindowMinimumNumberOfCalls(1);
    circuitBreakerConfig.setSlidingWindowSize(1);
    circuitBreakerConfig.setWaitDurationInOpenState(breakerWaitDuration);

    cluster = buildCluster(circuitBreakerConfig, ClientResources.builder());

    final String key = "key";

    // the first call should time out and open the breaker
    assertThrows(RedisCommandTimeoutException.class,
        () -> cluster.withCluster(connection -> connection.sync().blpop(2 * TIMEOUT.toMillis() / 1000d, key)));

    // the second call gets blocked by the breaker
    final RedisException e = assertThrows(RedisException.class,
        () -> cluster.withCluster(connection -> connection.sync().blpop(2 * TIMEOUT.toMillis() / 1000d, key)));
    assertInstanceOf(CallNotPermittedException.class, e.getCause());

    // wait for breaker to be half-open
    Thread.sleep(breakerWaitDuration.toMillis() * 2);

    assertEquals(0, (Long) cluster.withCluster(connection -> connection.sync().llen(key)));
  }

  @Test
  void testShardUnavailable() {
    final TestBreakerManager testBreakerManager = new TestBreakerManager();
    final CircuitBreakerConfiguration circuitBreakerConfig = new CircuitBreakerConfiguration();
    circuitBreakerConfig.setFailureRateThreshold(1);
    circuitBreakerConfig.setSlidingWindowMinimumNumberOfCalls(2);
    circuitBreakerConfig.setSlidingWindowSize(5);

    final ClientResources.Builder builder = CompositeNettyCustomizerClientResourcesBuilder.builder()
        .nettyCustomizer(testBreakerManager);

    cluster = buildCluster(circuitBreakerConfig, builder);

    // this test will open the breaker on one shard and check that other shards are still available,
    // so we get two nodes and a slot+key on each to test
    final Pair<RedisClusterNode, RedisClusterNode> nodePair =
        cluster.withCluster(connection -> {
          Partitions partitions = ClusterPartitionParser.parse(connection.sync().clusterNodes());

          assertTrue(partitions.size() >= 2);

          return new Pair<>(partitions.getPartition(0), partitions.getPartition(1));
        });

    final RedisClusterNode unavailableNode = nodePair.first();
    final int unavailableSlot = unavailableNode.getSlots().getFirst();
    final String unavailableKey = "key::{%s}".formatted(RedisClusterUtil.getMinimalHashTag(unavailableSlot));

    final int availableSlot = nodePair.second().getSlots().getFirst();
    final String availableKey = "key::{%s}".formatted(RedisClusterUtil.getMinimalHashTag(availableSlot));

    cluster.useCluster(connection -> {
      connection.sync().set(unavailableKey, "unavailable");
      connection.sync().set(availableKey, "available");

      assertEquals("unavailable", connection.sync().get(unavailableKey));
      assertEquals("available", connection.sync().get(availableKey));
    });

    // shard is now unavailable
    testBreakerManager.openBreaker(unavailableNode.getUri());
    final RedisException e = assertThrows(RedisException.class, () ->
        cluster.useCluster(connection -> connection.sync().get(unavailableKey)));
    assertInstanceOf(CallNotPermittedException.class, e.getCause());

    // other shard is still available
    assertEquals("available", cluster.withCluster(connection -> connection.sync().get(availableKey)));

    // shard is available again
    testBreakerManager.closeBreaker(unavailableNode.getUri());
    assertEquals("unavailable", cluster.withCluster(connection -> connection.sync().get(unavailableKey)));
  }

  @Test
  void testShardUnavailablePubSub() throws Exception {
    final TestBreakerManager testBreakerManager = new TestBreakerManager();
    final CircuitBreakerConfiguration circuitBreakerConfig = new CircuitBreakerConfiguration();
    circuitBreakerConfig.setFailureRateThreshold(1);
    circuitBreakerConfig.setSlidingWindowMinimumNumberOfCalls(2);
    circuitBreakerConfig.setSlidingWindowSize(5);

    final ClientResources.Builder builder = CompositeNettyCustomizerClientResourcesBuilder.builder()
        .nettyCustomizer(testBreakerManager);

    cluster = buildCluster(circuitBreakerConfig, builder);

    cluster.useCluster(
        connection -> connection.sync().upstream().commands().configSet("notify-keyspace-events", "K$glz"));

    // this test will open the breaker on one shard and check that other shards are still available,
    // so we get two nodes and a slot+key on each to test
    final Pair<RedisClusterNode, RedisClusterNode> nodePair =
        cluster.withCluster(connection -> {
          Partitions partitions = ClusterPartitionParser.parse(connection.sync().clusterNodes());

          assertTrue(partitions.size() >= 2);

          return new Pair<>(partitions.getPartition(0), partitions.getPartition(1));
        });

    final RedisClusterNode unavailableNode = nodePair.first();
    final int unavailableSlot = unavailableNode.getSlots().getFirst();
    final String unavailableKey = "key::{%s}".formatted(RedisClusterUtil.getMinimalHashTag(unavailableSlot));

    final RedisClusterNode availableNode = nodePair.second();
    final int availableSlot = availableNode.getSlots().getFirst();
    final String availableKey = "key::{%s}".formatted(RedisClusterUtil.getMinimalHashTag(availableSlot));

    final FaultTolerantPubSubClusterConnection<String, String> pubSubConnection = cluster.createPubSubConnection();

    // Keyspace notifications are delivered on a different thread, so we use a CountDownLatch to wait for the
    // expected number of notifications to arrive
    final AtomicReference<CountDownLatch> countDownLatchRef = new AtomicReference<>();

    final Map<String, AtomicInteger> channelMessageCounts = new ConcurrentHashMap<>();
    final String keyspacePrefix = "__keyspace@0__:";
    final RedisClusterPubSubAdapter<String, String> listener = new RedisClusterPubSubAdapter<>() {
      @Override
      public void message(final RedisClusterNode node, final String channel, final String message) {
        channelMessageCounts.computeIfAbsent(StringUtils.substringAfter(channel, keyspacePrefix),
                k -> new AtomicInteger(0))
            .incrementAndGet();

        countDownLatchRef.get().countDown();
      }
    };

    countDownLatchRef.set(new CountDownLatch(2));
    pubSubConnection.usePubSubConnection(c -> {
      c.addListener(listener);
      c.sync().nodes(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM) && node.hasSlot(availableSlot))
          .commands()
          .subscribe(keyspacePrefix + availableKey);
      c.sync().nodes(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM) && node.hasSlot(unavailableSlot))
          .commands()
          .subscribe(keyspacePrefix + unavailableKey);
    });

    cluster.useCluster(connection -> {
      connection.sync().set(availableKey, "ping1");
      connection.sync().set(unavailableKey, "ping1");
    });

    countDownLatchRef.get().await();

    assertEquals(1, channelMessageCounts.get(availableKey).get());
    assertEquals(1, channelMessageCounts.get(unavailableKey).get());

    // shard is now unavailable
    testBreakerManager.openBreaker(unavailableNode.getUri());

    final RedisException e = assertThrows(RedisException.class, () ->
        cluster.useCluster(connection -> connection.sync().set(unavailableKey, "ping2")));
    assertInstanceOf(CallNotPermittedException.class, e.getCause());
    assertEquals(1, channelMessageCounts.get(unavailableKey).get());
    assertEquals(1, channelMessageCounts.get(availableKey).get());

    countDownLatchRef.set(new CountDownLatch(1));
    pubSubConnection.usePubSubConnection(connection -> connection.sync().set(availableKey, "ping2"));

    countDownLatchRef.get().await();

    assertEquals(1, channelMessageCounts.get(unavailableKey).get());
    assertEquals(2, channelMessageCounts.get(availableKey).get());

    // shard is available again
    testBreakerManager.closeBreaker(unavailableNode.getUri());

    countDownLatchRef.set(new CountDownLatch(2));

    cluster.useCluster(connection -> {
      connection.sync().set(availableKey, "ping3");
      connection.sync().set(unavailableKey, "ping3");
    });

    countDownLatchRef.get().await();

    assertEquals(2, channelMessageCounts.get(unavailableKey).get());
    assertEquals(3, channelMessageCounts.get(availableKey).get());
  }

  @ChannelHandler.Sharable
  private static class TestBreakerManager extends ChannelDuplexHandler implements NettyCustomizer {

    private final Map<RedisURI, Set<LettuceShardCircuitBreaker.ChannelCircuitBreakerHandler>> urisToChannelBreakers = new ConcurrentHashMap<>();
    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public void afterChannelInitialized(Channel channel) {
      channel.pipeline().addFirst("TestBreakerManager#" + counter.getAndIncrement(), this);
    }

    @Override
    public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
        final SocketAddress localAddress, final ChannelPromise promise) throws Exception {

      super.connect(ctx, remoteAddress, localAddress, promise);

      final LettuceShardCircuitBreaker.ChannelCircuitBreakerHandler channelCircuitBreakerHandler =
          ctx.channel().pipeline().get(LettuceShardCircuitBreaker.ChannelCircuitBreakerHandler.class);

      urisToChannelBreakers.computeIfAbsent(getRedisURI(ctx.channel()), ignored -> new HashSet<>())
          .add(channelCircuitBreakerHandler);
    }

    private static RedisURI getRedisURI(Channel channel) {
      final InetSocketAddress inetAddress = (InetSocketAddress) channel.remoteAddress();
      return RedisURI.create(inetAddress.getHostString(), inetAddress.getPort());
    }

    void openBreaker(final RedisURI redisURI) {
      urisToChannelBreakers.get(redisURI).forEach(handler -> handler.breaker.transitionToOpenState());
    }

    void closeBreaker(final RedisURI redisURI) {
      urisToChannelBreakers.get(redisURI).forEach(handler -> handler.breaker.transitionToClosedState());
    }
  }

  static class CompositeNettyCustomizer implements NettyCustomizer {

    private final List<NettyCustomizer> nettyCustomizers = new ArrayList<>();

    @Override
    public void afterBootstrapInitialized(final Bootstrap bootstrap) {
      nettyCustomizers.forEach(nc -> nc.afterBootstrapInitialized(bootstrap));
    }

    @Override
    public void afterChannelInitialized(final Channel channel) {
      nettyCustomizers.forEach(nc -> nc.afterChannelInitialized(channel));
    }

    void add(NettyCustomizer customizer) {
      nettyCustomizers.add(customizer);
    }
  }

  static class CompositeNettyCustomizerClientResourcesBuilder implements ClientResources.Builder {

    private final CompositeNettyCustomizer compositeNettyCustomizer;
    private final ClientResources.Builder delegate;

    static CompositeNettyCustomizerClientResourcesBuilder builder() {
      return new CompositeNettyCustomizerClientResourcesBuilder();
    }

    private CompositeNettyCustomizerClientResourcesBuilder() {
      this.compositeNettyCustomizer = new CompositeNettyCustomizer();
      this.delegate = ClientResources.builder().nettyCustomizer(compositeNettyCustomizer);
    }


    @Override
    public ClientResources.Builder addressResolverGroup(final AddressResolverGroup<?> addressResolverGroup) {
      delegate.addressResolverGroup(addressResolverGroup);
      return this;
    }

    @Override
    public ClientResources.Builder commandLatencyRecorder(final CommandLatencyRecorder latencyRecorder) {
      delegate.commandLatencyRecorder(latencyRecorder);
      return this;
    }

    @Override
    @Deprecated
    public ClientResources.Builder commandLatencyCollectorOptions(
        final CommandLatencyCollectorOptions commandLatencyCollectorOptions) {
      delegate.commandLatencyCollectorOptions(commandLatencyCollectorOptions);
      return this;
    }

    @Override
    public ClientResources.Builder commandLatencyPublisherOptions(
        final EventPublisherOptions commandLatencyPublisherOptions) {
      delegate.commandLatencyPublisherOptions(commandLatencyPublisherOptions);
      return this;
    }

    @Override
    public ClientResources.Builder computationThreadPoolSize(final int computationThreadPoolSize) {
      delegate.computationThreadPoolSize(computationThreadPoolSize);
      return this;
    }

    @Override
    @Deprecated
    public ClientResources.Builder dnsResolver(final DnsResolver dnsResolver) {
      delegate.dnsResolver(dnsResolver);
      return this;
    }

    @Override
    public ClientResources.Builder eventBus(final EventBus eventBus) {
      delegate.eventBus(eventBus);
      return this;
    }

    @Override
    public ClientResources.Builder eventExecutorGroup(final EventExecutorGroup eventExecutorGroup) {
      delegate.eventExecutorGroup(eventExecutorGroup);
      return this;
    }

    @Override
    public ClientResources.Builder eventLoopGroupProvider(final EventLoopGroupProvider eventLoopGroupProvider) {
      delegate.eventLoopGroupProvider(eventLoopGroupProvider);
      return this;
    }

    @Override
    public ClientResources.Builder ioThreadPoolSize(final int ioThreadPoolSize) {
      delegate.ioThreadPoolSize(ioThreadPoolSize);
      return this;
    }

    @Override
    public ClientResources.Builder nettyCustomizer(final NettyCustomizer nettyCustomizer) {
      compositeNettyCustomizer.add(nettyCustomizer);
      return this;
    }

    @Override
    public ClientResources.Builder reconnectDelay(final Delay reconnectDelay) {
      delegate.reconnectDelay(reconnectDelay);
      return this;
    }

    @Override
    public ClientResources.Builder reconnectDelay(final Supplier<Delay> reconnectDelay) {
      delegate.reconnectDelay(reconnectDelay);
      return this;
    }

    @Override
    public ClientResources.Builder socketAddressResolver(final SocketAddressResolver socketAddressResolver) {
      delegate.socketAddressResolver(socketAddressResolver);
      return this;
    }

    @Override
    public ClientResources.Builder threadFactoryProvider(final ThreadFactoryProvider threadFactoryProvider) {
      delegate.threadFactoryProvider(threadFactoryProvider);
      return this;
    }

    @Override
    public ClientResources.Builder timer(final Timer timer) {
      delegate.timer(timer);
      return this;
    }

    @Override
    public ClientResources.Builder tracing(final Tracing tracing) {
      delegate.tracing(tracing);
      return this;
    }

    @Override
    public ClientResources build() {
      return delegate.build();
    }
  }

}
