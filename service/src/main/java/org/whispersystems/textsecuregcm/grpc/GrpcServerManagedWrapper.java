/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.dropwizard.lifecycle.Managed;
import io.grpc.Server;

public class GrpcServerManagedWrapper implements Managed {

  private final Server server;

  public GrpcServerManagedWrapper(final Server server) {
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
    } catch (InterruptedException e) {
      server.shutdownNow();
    }
  }
}
