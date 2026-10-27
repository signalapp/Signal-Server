/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalIoHandler;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.codec.http2.DefaultHttp2GoAwayFrame;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2GoAwayFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.test.LeakPresenceExtension;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LeakPresenceExtension.class)
class PooledH2ConnectionFactoryTest {

  private static EventLoopGroup backendEventLoopGroup;
  private static EventLoopGroup callerEventLoopGroup;

  private LocalAddress backendAddress;
  private Channel backendServer;
  private BlockingQueue<Channel> newBackendConnections;
  private ChannelGroup allBackendConnections;
  private PooledH2ConnectionFactory factory;

  @BeforeAll
  static void setUpBeforeAll() {
    backendEventLoopGroup = new MultiThreadIoEventLoopGroup(LocalIoHandler.newFactory());
    callerEventLoopGroup = new MultiThreadIoEventLoopGroup(2, LocalIoHandler.newFactory());
  }

  @BeforeEach
  void setUp() {
    backendAddress = new LocalAddress(PooledH2ConnectionFactoryTest.class);
    newBackendConnections = new LinkedBlockingQueue<>();
    allBackendConnections = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    startBackend();
    factory = new PooledH2ConnectionFactory(backendAddress, backendEventLoopGroup, Duration.ofDays(1));
  }

  @AfterEach
  void tearDown() {
    backendServer.close().syncUninterruptibly();
    allBackendConnections.close().syncUninterruptibly();
  }

  @AfterAll
  static void tearDownAfterAll() throws InterruptedException {
    backendEventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
    callerEventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
  }

  @Test
  void reuseConnection() throws Exception {
    final EventLoop eventLoop1 = callerEventLoopGroup.next();
    final EventLoop eventLoop2 = callerEventLoopGroup.next();
    final Channel connection = getConnection(eventLoop1).channel();
    assertSame(connection, getConnection(eventLoop1).channel());
    assertNotSame(connection, getConnection(eventLoop2).channel());
  }

  @Test
  void shareConnectionWhileConnecting() throws Exception {
    final EventLoop callerEventLoop = callerEventLoopGroup.next();
    final List<ChannelFuture> connectFutures = callerEventLoop.submit(() -> List.of(
            factory.getConnectionChannelFuture(callerEventLoop),
            factory.getConnectionChannelFuture(callerEventLoop)))
        .get();

    assertSame(connectFutures.get(0), connectFutures.get(1));
  }

  @Test
  void replaceClosedConnection() throws Exception {
    final EventLoop callerEventLoop = callerEventLoopGroup.next();
    final Channel connection = getConnection(callerEventLoop).channel();

    backendConnection().close().syncUninterruptibly();
    connection.closeFuture().syncUninterruptibly();

    final Channel replacement = getConnection(callerEventLoop).channel();
    assertNotSame(connection, replacement);
    assertTrue(replacement.isActive());
  }

  @Test
  void replaceFailedConnection() throws Exception {
    backendServer.close().syncUninterruptibly();

    final EventLoop callerEventLoop = callerEventLoopGroup.next();
    assertFalse(getConnection(callerEventLoop).isSuccess());

    startBackend();
    assertTrue(getConnection(callerEventLoop).isSuccess());
  }

  @Test
  void replaceConnectionAfterGoAway() throws Exception {
    final EventLoop callerEventLoop = callerEventLoopGroup.next();
    final Channel connection = getConnection(callerEventLoop).channel();

    final CompletableFuture<Void> goAwayRead = goAwayRead(connection);
    backendConnection().writeAndFlush(new DefaultHttp2GoAwayFrame(Http2Error.NO_ERROR)).syncUninterruptibly();
    goAwayRead.get(5, TimeUnit.SECONDS);

    assertNotSame(connection, getConnection(callerEventLoop).channel());
    assertTrue(connection.isActive());

    factory.release(connection);
    assertTrue(connection.closeFuture().await(5, TimeUnit.SECONDS));
  }

  @Test
  void retireConnection() throws Exception {
    factory = new PooledH2ConnectionFactory(backendAddress, backendEventLoopGroup, Duration.ZERO);

    final EventLoop callerEventLoop = callerEventLoopGroup.next();
    final Channel connection = getConnection(callerEventLoop).channel();

    assertNotSame(connection, getConnection(callerEventLoop).channel());
    assertTrue(connection.isActive());

    factory.release(connection);
    assertTrue(connection.closeFuture().await(5, TimeUnit.SECONDS));
  }

  @Test
  void closeUnusedConnectionWhenReplaced() throws Exception {
    factory = new PooledH2ConnectionFactory(backendAddress, backendEventLoopGroup, Duration.ZERO);

    final EventLoop callerEventLoop = callerEventLoopGroup.next();
    final Channel connection = getConnection(callerEventLoop).channel();
    factory.release(connection);
    assertTrue(connection.isActive());

    getConnection(callerEventLoop);
    assertTrue(connection.closeFuture().await(5, TimeUnit.SECONDS));
  }

  private void startBackend() {
    backendServer = new ServerBootstrap()
        .group(backendEventLoopGroup)
        .channel(LocalServerChannel.class)
        .childHandler(new ChannelInitializer<>() {
          @Override
          protected void initChannel(final Channel ch) {
            ch.pipeline().addLast(Http2FrameCodecBuilder.forServer().build());
            ch.pipeline().addLast(new Http2MultiplexHandler(new ChannelInboundHandlerAdapter()));
            newBackendConnections.add(ch);
            allBackendConnections.add(ch);
          }
        })
        .bind(backendAddress)
        .syncUninterruptibly()
        .channel();
  }

  /// Get a connection from the factory on `callerEventLoop`, waiting for it to finish connecting
  private ChannelFuture getConnection(final EventLoop callerEventLoop) throws Exception {
    return callerEventLoop.submit(() -> factory.getConnectionChannelFuture(callerEventLoop)).get().await();
  }

  /// The backend's side of the next connection the factory made
  private Channel backendConnection() throws InterruptedException {
    final Channel backendConnection = newBackendConnections.poll(5, TimeUnit.SECONDS);
    assertNotNull(backendConnection);
    return backendConnection;
  }

  /// A future that completes once the factory has seen a GOAWAY on `connection`
  private static CompletableFuture<Void> goAwayRead(final Channel connection) {
    final CompletableFuture<Void> goAwayRead = new CompletableFuture<>();
    connection.pipeline().addAfter(connection.pipeline().context(Http2MultiplexHandler.class).name(), null,
        new ChannelInboundHandlerAdapter() {
          @Override
          public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            ctx.fireChannelRead(msg);
            if (msg instanceof Http2GoAwayFrame) {
              goAwayRead.complete(null);
            }
          }
        });
    return goAwayRead;
  }
}
