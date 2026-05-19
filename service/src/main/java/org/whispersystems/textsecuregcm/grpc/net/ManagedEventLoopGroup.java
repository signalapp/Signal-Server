/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.netty4.NettyEventExecutorMetrics;
import io.netty.channel.EventLoopGroup;

/**
 * A wrapper for a Netty {@link EventLoopGroup} that implements Dropwizard's {@link Managed} interface, allowing
 * Dropwizard to manage the lifecycle of the event loop group.
 */
public class ManagedEventLoopGroup<T extends EventLoopGroup> implements Managed {

  private final T eventLoopGroup;

  public ManagedEventLoopGroup(final T eventLoopGroup) {
    this.eventLoopGroup = eventLoopGroup;
  }

  @Override
  public void start() {
    new NettyEventExecutorMetrics(eventLoopGroup).bindTo(Metrics.globalRegistry);
  }

  @Override
  public void stop() throws Exception {
    this.eventLoopGroup.shutdownGracefully().await();
  }

  public T getEventLoopGroup() {
    return eventLoopGroup;
  }
}
