/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http2.DefaultHttp2WindowUpdateFrame;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2GoAwayFrame;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.whispersystems.textsecuregcm.util.ClosableEpoch;

/// An [H2ConnectionFactory] that creates one shared h2 connection per calling event loop.
///
/// A connection is replaced the next time it's requested if it failed to connect, has closed, has received a GOAWAY, or
/// is older than `retireAfterNanos`. Retiring connections bounds how many stream IDs a connection uses since eventually
/// stream-ids are exhausted.
///
/// A replaced connection is never handed out again, and is closed once every stream it was handed out for has been
/// [#release]ed.
public class PooledH2ConnectionFactory implements H2ConnectionFactory {

  private static final AttributeKey<Boolean> GOAWAY_RECEIVED =
      AttributeKey.valueOf(PooledH2ConnectionFactory.class, "goawayReceived");

  // Key for a [ClosableEpoch] attached to every new connection's channel. When the connection is replaced,
  // [ClosableEpoch#close] is called, and when all streams are released the underlying netty channel is closed.
  private static final AttributeKey<ClosableEpoch> CONNECTION_EPOCH =
      AttributeKey.valueOf(PooledH2ConnectionFactory.class, "connection_epoch");

  private static final PooledConnectionHandler POOLED_CONNECTION_HANDLER = new PooledConnectionHandler();

  private static final String CONNECTION_REPLACED_COUNTER_NAME =
      name(PooledH2ConnectionFactory.class, "connectionReplaced");
  private static final String OPEN_CONNECTIONS_GAUGE_NAME = name(PooledH2ConnectionFactory.class, "openConnections");

  // How long to use a pooled connection for
  private static final Duration RETIRE_AFTER = Duration.ofMinutes(30);

  private final UnpooledH2ConnectionFactory connectionFactory;
  private final long retireAfterNanos;

  private final ConcurrentHashMap<EventLoop, PooledConnection> connectionsByCallerEventLoop = new ConcurrentHashMap<>();
  // Count of all open connections (including replaced ones waiting for outstanding streams to terminate)
  private final AtomicInteger openConnections;

  private record PooledConnection(ChannelFuture connectFuture, long createdNanos) {}

  /// Create a pooled H2 connection factory
  ///
  /// @param targetAddress The [SocketAddress] connections should target
  /// @param eventLoopGroup The eventLoopGroup on which connections will be made. Must match the type of the [SocketAddress]
  public PooledH2ConnectionFactory(final SocketAddress targetAddress, final EventLoopGroup eventLoopGroup) {
    this(targetAddress, eventLoopGroup, RETIRE_AFTER);
  }

  @VisibleForTesting
  PooledH2ConnectionFactory(final SocketAddress targetAddress,
      final EventLoopGroup eventLoopGroup,
      final Duration retireAfter) {
    this.connectionFactory = new UnpooledH2ConnectionFactory(targetAddress, eventLoopGroup, POOLED_CONNECTION_HANDLER);
    this.retireAfterNanos = retireAfter.toNanos();
    this.openConnections = Metrics.gauge(OPEN_CONNECTIONS_GAUGE_NAME, Tags.of("backend", targetAddress.toString()),
        new AtomicInteger());
  }

  @Override
  public ChannelFuture getConnectionChannelFuture(final EventLoop callerEventLoop) {
    assert callerEventLoop.inEventLoop();

    final ChannelFuture connection = connectionsByCallerEventLoop.compute(callerEventLoop, (_, existing) -> {
          if (existing == null) {
            return newConnection(callerEventLoop);
          } else if (!isUsable(existing)) {
            existing.connectFuture().channel().attr(CONNECTION_EPOCH).get().close();
            return newConnection(callerEventLoop);
          } else {
            return existing;
          }
        })
        .connectFuture();

    // Compute always hands us a connection with an open epoch and we can't race with anyone because we're in
    // callerEventLoop, so we should always be able to enter the epoch
    final boolean arrived = connection.channel().attr(CONNECTION_EPOCH).get().tryArrive();
    assert arrived;

    return connection;
  }

  private PooledConnection newConnection(final EventLoop callerEventLoop) {
    final ChannelFuture connectFuture = connectionFactory.getConnectionChannelFuture(callerEventLoop);
    final Channel channel = connectFuture.channel();
    channel.attr(CONNECTION_EPOCH).set(new ClosableEpoch(channel::close));
    openConnections.incrementAndGet();
    channel.closeFuture().addListener(_ -> openConnections.decrementAndGet());
    return new PooledConnection(connectFuture, System.nanoTime());
  }

  /// Whether a pooled connection can still be handed out
  private boolean isUsable(final PooledConnection pooledConnection) {
    final ChannelFuture connectionFuture = pooledConnection.connectFuture();
    if (!connectionFuture.isDone()) {
      return true;
    }

    final Channel connection = connectionFuture.channel();

    // We know the future is done, see if we actually connected or if we've since disconnected
    final boolean active = connection.isActive();

    // We might still be connected but have gotten a GOAWAY indicating we shouldn't open new streams
    final boolean goawayReceived = Boolean.TRUE.equals(connection.attr(GOAWAY_RECEIVED).get());

    // The connection might be fine but old enough that we want a fresh one
    final boolean retired = System.nanoTime() - pooledConnection.createdNanos() > retireAfterNanos;

    final boolean usable = active && !goawayReceived && !retired;
    if (!usable) {
      Metrics.counter(CONNECTION_REPLACED_COUNTER_NAME,
              "backend", address().toString(),
              "active", String.valueOf(active),
              "goaway", String.valueOf(goawayReceived),
              "retired", String.valueOf(retired))
          .increment();
    }
    return usable;
  }

  @Override
  public void release(final Channel connection) {
    connection.attr(CONNECTION_EPOCH).get().depart();
  }

  @Override
  public SocketAddress address() {
    return connectionFactory.address();
  }

  /// Initializes the connection with a large window, and marks a connection as draining when the backend sends a GOAWAY
  @ChannelHandler.Sharable
  private static class PooledConnectionHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
      // Every new H2 connection starts with a connection flow-control window of 64K. Each stream on the connection also
      // starts with a per-stream window of 64K. See https://www.rfc-editor.org/rfc/rfc9113.html#section-6.9.2.
      //
      // Because we're pooling many distinct, h2 streams from different remote clients may share this single h2
      // connection to the backend. So, by default, if a single stream is slow to receive a large response from the
      // backend, they could consume the entire connection window.
      //
      // To avoid this, as soon as we open the connection tell the server we're willing to accept more at the connection
      // level. We update the connection window to the max value by sending a WINDOW_UPDATE (6.9) to add enough credit
      // to reach the max connection window size of INT_MAX. The individual streams still retain the default window-size
      // of 64K.
      ctx.writeAndFlush(new DefaultHttp2WindowUpdateFrame(Integer.MAX_VALUE - Http2CodecUtil.DEFAULT_WINDOW_SIZE));
      ctx.fireChannelActive();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
      if (msg instanceof Http2GoAwayFrame) {
        ctx.channel().attr(GOAWAY_RECEIVED).set(true);
        ReferenceCountUtil.release(msg);
        return;
      }
      ctx.fireChannelRead(msg);
    }
  }
}
