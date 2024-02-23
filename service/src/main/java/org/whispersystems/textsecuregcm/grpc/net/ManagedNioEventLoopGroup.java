package org.whispersystems.textsecuregcm.grpc.net;

import io.dropwizard.lifecycle.Managed;
import io.netty.channel.nio.NioEventLoopGroup;

/**
 * A wrapper for a Netty {@link NioEventLoopGroup} that implements Dropwizard's {@link Managed} interface, allowing
 * Dropwizard to manage the lifecycle of the event loop group.
 */
public class ManagedNioEventLoopGroup extends NioEventLoopGroup implements Managed {

  @Override
  public void stop() throws Exception {
    this.shutdownGracefully().await();
  }
}
