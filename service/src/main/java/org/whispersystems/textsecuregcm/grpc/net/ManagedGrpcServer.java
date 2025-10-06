package org.whispersystems.textsecuregcm.grpc.net;

import io.dropwizard.lifecycle.Managed;
import io.grpc.Server;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ManagedGrpcServer implements Managed {
  private final Server server;

  public ManagedGrpcServer(Server server) {
    this.server = server;
  }

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
