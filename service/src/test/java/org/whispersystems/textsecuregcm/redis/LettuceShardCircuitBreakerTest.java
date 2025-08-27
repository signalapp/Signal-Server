/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandHandler;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.Endpoint;
import io.lettuce.core.resource.ClientResources;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class LettuceShardCircuitBreakerTest {

  private LettuceShardCircuitBreaker.ChannelCircuitBreakerHandler channelCircuitBreakerHandler;

  @BeforeEach
  void setUp() {
    channelCircuitBreakerHandler = new LettuceShardCircuitBreaker.ChannelCircuitBreakerHandler("test", null);
  }

  @Test
  void testAfterChannelInitialized() {

    final LettuceShardCircuitBreaker lettuceShardCircuitBreaker =
        new LettuceShardCircuitBreaker("test", null);

    final Channel channel = new EmbeddedChannel(
        new CommandHandler(ClientOptions.create(), ClientResources.create(), mock(Endpoint.class)));

    lettuceShardCircuitBreaker.afterChannelInitialized(channel);

    final AtomicBoolean foundCommandHandler = new AtomicBoolean(false);
    final AtomicBoolean foundChannelCircuitBreakerHandler = new AtomicBoolean(false);
    StreamSupport.stream(channel.pipeline().spliterator(), false)
        .forEach(nameAndHandler -> {
          if (nameAndHandler.getValue() instanceof CommandHandler) {
            foundCommandHandler.set(true);
          }
          if (nameAndHandler.getValue() instanceof LettuceShardCircuitBreaker.ChannelCircuitBreakerHandler) {
            foundChannelCircuitBreakerHandler.set(true);
          }
          if (foundCommandHandler.get()) {
            assertTrue(foundChannelCircuitBreakerHandler.get(),
                "circuit breaker handler should be before the command handler");
          }
        });

    assertTrue(foundChannelCircuitBreakerHandler.get());
    assertTrue(foundCommandHandler.get());
  }

  @Test
  void testHandlerConnect() throws Exception {
    channelCircuitBreakerHandler.connect(mock(ChannelHandlerContext.class), mock(SocketAddress.class),
        mock(SocketAddress.class), mock(ChannelPromise.class));

    assertNotNull(channelCircuitBreakerHandler.breaker);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHandlerWriteBreakerClosed(final boolean completeExceptionally) throws Exception {
    final CircuitBreaker breaker = mock(CircuitBreaker.class);
    channelCircuitBreakerHandler.breaker = breaker;

    final AsyncCommand<String, String, String> command = new AsyncCommand<>(
        new Command<>(CommandType.PING, new StatusOutput<>(StringCodec.ASCII)));
    final ChannelHandlerContext channelHandlerContext = mock(ChannelHandlerContext.class);
    final ChannelPromise channelPromise = mock(ChannelPromise.class);
    channelCircuitBreakerHandler.write(channelHandlerContext, command, channelPromise);

    verify(breaker).acquirePermission();

    if (completeExceptionally) {
      final Throwable throwable = new IOException("timeout");

      command.completeExceptionally(throwable);
      verify(breaker).onError(anyLong(), eq(TimeUnit.NANOSECONDS), eq(throwable));
    } else {
      command.complete("PONG");
      verify(breaker).onSuccess(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    // write should always be forwarded when the breaker is closed
    verify(channelHandlerContext).write(command, channelPromise);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHandlerWriteBatchBreakerClosed(final boolean completeExceptionally) throws Exception {
    final CircuitBreaker breaker = mock(CircuitBreaker.class);
    channelCircuitBreakerHandler.breaker = breaker;

    final AsyncCommand<String, String, String> firstCommand = new AsyncCommand<>(
        new Command<>(CommandType.PING, new StatusOutput<>(StringCodec.ASCII)));
    final AsyncCommand<String, String, String> secondCommand = new AsyncCommand<>(
        new Command<>(CommandType.PING, new StatusOutput<>(StringCodec.ASCII)));
    final ChannelHandlerContext channelHandlerContext = mock(ChannelHandlerContext.class);
    final ChannelPromise channelPromise = mock(ChannelPromise.class);
    channelCircuitBreakerHandler.write(channelHandlerContext, List.of(firstCommand, secondCommand), channelPromise);

    verify(breaker).acquirePermission();

    if (completeExceptionally) {
      final Throwable throwable = new IOException("timeout");

      firstCommand.completeExceptionally(throwable);
      secondCommand.completeExceptionally(throwable);
      verify(breaker).onError(anyLong(), eq(TimeUnit.NANOSECONDS), eq(throwable));
    } else {
      firstCommand.complete("PONG");
      secondCommand.complete("PONG");
      verify(breaker).onSuccess(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    // write should always be forwarded when the breaker is closed
    verify(channelHandlerContext).write(List.of(firstCommand, secondCommand), channelPromise);
  }

  @Test
  void testHandlerWriteBreakerOpen() throws Exception {
    final CircuitBreaker breaker = mock(CircuitBreaker.class);
    channelCircuitBreakerHandler.breaker = breaker;

    final CallNotPermittedException callNotPermittedException = mock(CallNotPermittedException.class);
    doThrow(callNotPermittedException).when(breaker).acquirePermission();

    @SuppressWarnings("unchecked") final AsyncCommand<String, String, String> command = mock(AsyncCommand.class);
    final ChannelHandlerContext channelHandlerContext = mock(ChannelHandlerContext.class);
    final ChannelPromise channelPromise = mock(ChannelPromise.class);
    channelCircuitBreakerHandler.write(channelHandlerContext, command, channelPromise);

    verify(command).completeExceptionally(callNotPermittedException);
    verify(channelPromise).tryFailure(callNotPermittedException);

    verifyNoInteractions(channelHandlerContext);
  }

  @Test
  void testHandlerWriteBatchBreakerOpen() throws Exception {
    final CircuitBreaker breaker = mock(CircuitBreaker.class);
    channelCircuitBreakerHandler.breaker = breaker;

    final CallNotPermittedException callNotPermittedException = mock(CallNotPermittedException.class);
    doThrow(callNotPermittedException).when(breaker).acquirePermission();

    @SuppressWarnings("unchecked") final AsyncCommand<String, String, String> firstCommand = mock(AsyncCommand.class);
    @SuppressWarnings("unchecked") final AsyncCommand<String, String, String> secondCommand = mock(AsyncCommand.class);
    final ChannelHandlerContext channelHandlerContext = mock(ChannelHandlerContext.class);
    final ChannelPromise channelPromise = mock(ChannelPromise.class);
    channelCircuitBreakerHandler.write(channelHandlerContext, List.of(firstCommand, secondCommand), channelPromise);

    verify(firstCommand).completeExceptionally(callNotPermittedException);
    verify(secondCommand).completeExceptionally(callNotPermittedException);
    verify(channelPromise).tryFailure(callNotPermittedException);

    verifyNoInteractions(channelHandlerContext);
  }
}
