package org.whispersystems.textsecuregcm.grpc.net;

import io.dropwizard.lifecycle.Managed;
import io.netty.channel.DefaultEventLoopGroup;

/**
 * A wrapper for a Netty {@link DefaultEventLoopGroup} that implements Dropwizard's {@link Managed} interface, allowing
 * Dropwizard to manage the lifecycle of the event loop group.
 */
public class ManagedDefaultEventLoopGroup extends DefaultEventLoopGroup implements Managed {

  @Override
  public void stop() throws InterruptedException {
    this.shutdownGracefully().await();
  }
}
