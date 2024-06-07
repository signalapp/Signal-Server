package org.whispersystems.textsecuregcm.util.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

/**
 * Filters spurious warnings about setting a very specific channel option on local channels.
 * <p/>
 * gRPC unconditionally tries to set the {@code SO_KEEPALIVE} option on all of its channels, but local channels, which
 * are used by the Noise-over-WebSocket tunnel, do not support {@code SO_KEEPALIVE} and log a warning on each new
 * channel. We don't want to filter <em>all</em> warnings from the relevant logger, and so this custom filter denies
 * attempts to log the specific spurious message.
 */
public class UnknownKeepaliveOptionFilter extends Filter<ILoggingEvent> {

  private static final String MESSAGE_PREFIX = "Unknown channel option 'SO_KEEPALIVE'";

  @Override
  public FilterReply decide(final ILoggingEvent event) {
    final boolean loggerNameMatches = "io.netty.bootstrap.Bootstrap".equals(event.getLoggerName()) ||
        "io.netty.bootstrap.ServerBootstrap".equals(event.getLoggerName());

    return loggerNameMatches && event.getMessage().startsWith(MESSAGE_PREFIX) ? FilterReply.DENY : FilterReply.NEUTRAL;
  }
}
