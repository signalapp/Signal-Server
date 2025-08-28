/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.protocol.CommandHandler;
import io.lettuce.core.protocol.CompleteableCommand;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.resource.NettyCustomizer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;

/**
 * Adds a circuit breaker to every Netty {@link Channel} that gets created, so that a single unhealthy shard does not
 * impact all cluster operations.
 * <p>
 * For metrics to be registered, users <em>must</em> create a synthetic {@link ClusterTopologyChangedEvent} after the
 * initial connection. For example:
 * <pre>
 *   clusterClient.connect();
 *   clusterClient.getResources().eventBus().publish(
 *         new ClusterTopologyChangedEvent(Collections.emptyList(), clusterClient.getPartitions().getPartitions()));
 * </pre>
 */
public class LettuceShardCircuitBreaker implements NettyCustomizer {

  private static final Logger logger = LoggerFactory.getLogger(LettuceShardCircuitBreaker.class);

  private final String clusterName;
  @Nullable
  private final String circuitBreakerConfigurationName;

  public LettuceShardCircuitBreaker(final String clusterName, @Nullable final String circuitBreakerConfigurationName) {
    this.clusterName = clusterName;
    this.circuitBreakerConfigurationName = circuitBreakerConfigurationName;
  }

  @Override
  public void afterChannelInitialized(final Channel channel) {
    final ChannelCircuitBreakerHandler channelCircuitBreakerHandler =
        new ChannelCircuitBreakerHandler(clusterName, circuitBreakerConfigurationName);

    final String commandHandlerName = StreamSupport.stream(channel.pipeline().spliterator(), false)
        .filter(entry -> entry.getValue() instanceof CommandHandler)
        .map(Map.Entry::getKey)
        .findFirst()
        .orElseThrow();
    channel.pipeline().addBefore(commandHandlerName, null, channelCircuitBreakerHandler);
  }

  static final class ChannelCircuitBreakerHandler extends ChannelOutboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ChannelCircuitBreakerHandler.class);

    private static final String CLUSTER_TAG_NAME = "cluster";
    private static final String SHARD_ADDRESS_TAG_NAME = "shard";

    private final String clusterName;
    @Nullable private final String circuitBreakerConfigurationName;

    private String shardAddress;

    @VisibleForTesting
    CircuitBreaker breaker;

    public ChannelCircuitBreakerHandler(final String name, @Nullable final String circuitBreakerConfigurationName) {
      this.clusterName = name;
      this.circuitBreakerConfigurationName = circuitBreakerConfigurationName;
    }

    @Override
    public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
        final SocketAddress localAddress, final ChannelPromise promise) throws Exception {
      super.connect(ctx, remoteAddress, localAddress, promise);
      // Unfortunately, the Channel's remote address is null until connect() is called, so we have to wait to initialize
      // the breaker with the remote’s name.
      // There is a Channel attribute, io.lettuce.core.ConnectionBuilder.REDIS_URI, but this does not always
      // match remote address, as it is inherited from the Bootstrap attributes and not updated for the Channel connection

      // In some cases, like the default connection, the remote address includes the DNS hostname, which we want to exclude.
      shardAddress = StringUtils.substringAfter(remoteAddress.toString(), "/");

      final String circuitBreakerName =
          ResilienceUtil.name(LettuceShardCircuitBreaker.class, "%s/%s".formatted(clusterName, shardAddress));

      final Map<String, String> tags = Map.of(
          CLUSTER_TAG_NAME, clusterName,
          SHARD_ADDRESS_TAG_NAME, shardAddress);

      breaker = circuitBreakerConfigurationName != null
          ? ResilienceUtil.getCircuitBreakerRegistry().circuitBreaker(circuitBreakerName, circuitBreakerConfigurationName, tags)
          : ResilienceUtil.getCircuitBreakerRegistry().circuitBreaker(circuitBreakerName, tags);
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise)
        throws Exception {

      logger.trace("Breaker state is {}", breaker.getState());

      // There are two types of RedisCommands that are not CompleteableCommand:
      // - io.lettuce.core.protocol.Command
      // - io.lettuce.core.protocol.PristineFallbackCommand
      //
      // The former always get wrapped by one of the other command types, and the latter is only used in an edge case
      // to consume responses.
      if (msg instanceof RedisCommand<?, ?, ?> rc && rc instanceof CompleteableCommand<?> command) {
        try {
          instrumentCommand(command);
        } catch (final CallNotPermittedException e) {
          rc.completeExceptionally(e);
          promise.tryFailure(e);
          return;
        }

      } else if (msg instanceof Collection<?> collection &&
          !collection.isEmpty() &&
          collection.stream().allMatch(obj -> obj instanceof RedisCommand && obj instanceof CompleteableCommand<?>)) {

        @SuppressWarnings("unchecked") final Collection<RedisCommand<?, ?, ?>> commandCollection =
            (Collection<RedisCommand<?, ?, ?>>) collection;

        try {
          // If we have a collection of commands, we only acquire a single permit for the whole batch (since there's
          // only a single write promise to fail). We choose a single command from the collection to sample for failure.
          instrumentCommand((CompleteableCommand<?>) commandCollection.iterator().next());
        } catch (final CallNotPermittedException e) {
          commandCollection.forEach(redisCommand -> redisCommand.completeExceptionally(e));
          promise.tryFailure(e);
          return;
        }
      } else {
        logger.warn("Unexpected msg type: {}", msg.getClass());
      }

      super.write(ctx, msg, promise);
    }

    private void instrumentCommand(final CompleteableCommand<?> command) throws CallNotPermittedException {
      breaker.acquirePermission();

      // state can change in acquirePermission()
      logger.trace("Breaker is permitted: {}", breaker.getState());

      final long startNanos = System.nanoTime();

      command.onComplete((ignored, throwable) -> {
        final long durationNanos = System.nanoTime() - startNanos;

        // RedisNoScriptException doesn’t indicate a fault the breaker can protect
        if (throwable != null && !(throwable instanceof RedisNoScriptException)) {
          breaker.onError(durationNanos, TimeUnit.NANOSECONDS, throwable);
          logger.warn("Command completed with error for: {}/{}", clusterName, shardAddress, throwable);
        } else {
          breaker.onSuccess(durationNanos, TimeUnit.NANOSECONDS);
        }
      });
    }
  }
}
