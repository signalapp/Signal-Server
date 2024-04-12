/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.lettuce.core.protocol.CommandHandler;
import io.lettuce.core.protocol.CompleteableCommand;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.resource.NettyCustomizer;
import io.micrometer.core.instrument.Tags;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;

public class LettuceShardCircuitBreaker implements NettyCustomizer {

  private final String clusterName;
  private final CircuitBreakerConfig circuitBreakerConfig;

  public LettuceShardCircuitBreaker(final String clusterName, final CircuitBreakerConfig circuitBreakerConfig) {
    this.clusterName = clusterName;
    this.circuitBreakerConfig = circuitBreakerConfig;
  }

  @Override
  public void afterChannelInitialized(final Channel channel) {

    final ChannelCircuitBreakerHandler channelCircuitBreakerHandler = new ChannelCircuitBreakerHandler(clusterName,
        circuitBreakerConfig);

    final String commandHandlerName = StreamSupport.stream(channel.pipeline().spliterator(), false)
        .filter(entry -> entry.getValue() instanceof CommandHandler)
        .map(Map.Entry::getKey)
        .findFirst()
        .orElseThrow();
    channel.pipeline().addBefore(commandHandlerName, null, channelCircuitBreakerHandler);
  }

  static final class ChannelCircuitBreakerHandler extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(ChannelCircuitBreakerHandler.class);

    private static final String SHARD_TAG_NAME = "shard";
    private static final String CLUSTER_TAG_NAME = "cluster";

    private final String clusterName;
    private final CircuitBreakerConfig circuitBreakerConfig;

    @VisibleForTesting
    CircuitBreaker breaker;

    public ChannelCircuitBreakerHandler(final String name, CircuitBreakerConfig circuitBreakerConfig) {
      this.clusterName = name;
      this.circuitBreakerConfig = circuitBreakerConfig;
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
      final String shardAddress = StringUtils.substringAfter(remoteAddress.toString(), "/");
      breaker = CircuitBreaker.of("%s/%s-breaker".formatted(clusterName, shardAddress), circuitBreakerConfig);
      CircuitBreakerUtil.registerMetrics(breaker, getClass(),
          Tags.of(CLUSTER_TAG_NAME, clusterName, SHARD_TAG_NAME, shardAddress));
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise)
        throws Exception {

      logger.trace("Breaker state is {}", breaker.getState());

      // Note: io.lettuce.core.protocol.CommandHandler also supports batches (List/Collection<RedisCommand>),
      // but we do not use that feature, so we can just check for single commands
      //
      // There are two types of RedisCommands that are not CompleteableCommand:
      // - io.lettuce.core.protocol.Command
      // - io.lettuce.core.protocol.PristineFallbackCommand
      //
      // The former always get wrapped by one of the other command types, and the latter is only used in an edge case
      // to consume responses.
      if (msg instanceof RedisCommand<?, ?, ?> rc && rc instanceof CompleteableCommand<?> command) {
        try {
          breaker.acquirePermission();

          // state can change in acquirePermission()
          logger.trace("Breaker is permitted: {}", breaker.getState());

          final long startNanos = System.nanoTime();

          command.onComplete((ignored, throwable) -> {
            final long durationNanos = System.nanoTime() - startNanos;

            if (throwable != null) {
              breaker.onError(durationNanos, TimeUnit.NANOSECONDS, throwable);
              logger.debug("Command completed with error", throwable);
            } else {
              breaker.onSuccess(durationNanos, TimeUnit.NANOSECONDS);
            }
          });

        } catch (final CallNotPermittedException e) {
          rc.completeExceptionally(e);
          promise.tryFailure(e);
          return;
        }

      } else {
        logger.warn("Unexpected msg type: {}", msg.getClass());
      }

      super.write(ctx, msg, promise);
    }
  }
}
