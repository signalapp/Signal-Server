/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import io.micrometer.core.instrument.Metrics;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

/// A handler that closes the channel on an exception and records errors in a counter. This should be placed at the tail
/// of pipelines to catch uncaught exceptions gracefully
@ChannelHandler.Sharable
public class OmnibusExceptionHandler extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(OmnibusExceptionHandler.class);

  private static final String UNCAUGHT_EXCEPTION_COUNTER_NAME = MetricsUtil.name(OmnibusExceptionHandler.class,
      "uncaughtException");

  private final String channelName;
  private final List<Class<? extends Exception>> expectedExceptions;

  public OmnibusExceptionHandler(final String channelName, final List<Class<? extends Exception>> expectedExceptions) {
    this.channelName = channelName;
    this.expectedExceptions = expectedExceptions;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    Metrics.counter(UNCAUGHT_EXCEPTION_COUNTER_NAME,
            "channelName", channelName,
            "exceptionClass", cause.getClass().getSimpleName())
        .increment();

    // There are 'expected' ways to get exceptions on a channel (e.g. client disconnects) so we only log them at debug.
    if (expectedException(cause)) {
      logger.debug("uncaught exception on channel {}", channelName, cause);
    } else {
      logger.warn("unexpected uncaught exception on channel {}", channelName, cause);
    }
    ctx.close();
  }

  private boolean expectedException(final Throwable exception) {
    return expectedExceptions
        .stream()
        .anyMatch(expectedException -> expectedException.isInstance(exception));
  }
}
