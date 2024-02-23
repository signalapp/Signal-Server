package org.whispersystems.textsecuregcm.grpc.net;

import io.dropwizard.lifecycle.Managed;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalServerChannel;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A managed, local gRPC server configures and wraps a gRPC {@link Server} that listens on a Netty {@link LocalAddress}
 * and whose lifecycle is managed by Dropwizard via the {@link Managed} interface.
 */
public abstract class ManagedLocalGrpcServer implements Managed {

  private final Server server;

  public ManagedLocalGrpcServer(final LocalAddress localAddress,
                                final DefaultEventLoopGroup eventLoopGroup) {

    final ServerBuilder<?> serverBuilder = NettyServerBuilder.forAddress(localAddress)
        .channelType(LocalServerChannel.class)
        .bossEventLoopGroup(eventLoopGroup)
        .workerEventLoopGroup(eventLoopGroup);

    configureServer(serverBuilder);

    server = serverBuilder.build();
  }

  protected abstract void configureServer(final ServerBuilder<?> serverBuilder);

  @Override
  public void start() throws IOException {
    server.start();
  }

  @Override
  public void stop() {
    try {
      server.shutdown().awaitTermination(5, TimeUnit.MINUTES);
    } catch (final InterruptedException e) {
      server.shutdownNow();
    }
  }
}
