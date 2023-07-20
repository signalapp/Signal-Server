/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.util.MutableHandlerRegistry;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

// This is mostly a direct port of
// https://github.com/grpc/grpc-java/blob/master/testing/src/main/java/io/grpc/testing/GrpcServerRule.java, but for
// JUnit 5.
public class GrpcServerExtension implements BeforeEachCallback, AfterEachCallback {

  private ManagedChannel channel;
  private Server server;
  private String serverName;
  private MutableHandlerRegistry serviceRegistry;
  private boolean useDirectExecutor;

  /**
   * Returns {@code this} configured to use a direct executor for the {@link ManagedChannel} and
   * {@link Server}. This can only be called at the rule instantiation.
   */
  public final GrpcServerExtension directExecutor() {
    if (serverName != null) {
      throw new IllegalStateException("directExecutor() can only be called at the rule instantiation");
    }

    useDirectExecutor = true;
    return this;
  }

  /**
   * Returns a {@link ManagedChannel} connected to this service.
   */
  public final ManagedChannel getChannel() {
    return channel;
  }

  /**
   * Returns the underlying gRPC {@link Server} for this service.
   */
  public final Server getServer() {
    return server;
  }

  /**
   * Returns the randomly generated server name for this service.
   */
  public final String getServerName() {
    return serverName;
  }

  /**
   * Returns the service registry for this service. The registry is used to add service instances
   * (e.g. {@link BindableService} or {@link ServerServiceDefinition} to the server.
   */
  public final MutableHandlerRegistry getServiceRegistry() {
    return serviceRegistry;
  }

  @Override
  public void beforeEach(final ExtensionContext extensionContext) throws Exception {
    serverName = UUID.randomUUID().toString();
    serviceRegistry = new MutableHandlerRegistry();

    final InProcessServerBuilder serverBuilder = InProcessServerBuilder.forName(serverName)
        .fallbackHandlerRegistry(serviceRegistry);

    if (useDirectExecutor) {
      serverBuilder.directExecutor();
    }

    server = serverBuilder.build().start();

    final InProcessChannelBuilder channelBuilder = InProcessChannelBuilder.forName(serverName);

    if (useDirectExecutor) {
      channelBuilder.directExecutor();
    }

    channel = channelBuilder.build();
  }

  @Override
  public void afterEach(final ExtensionContext extensionContext) throws Exception {
    serverName = null;
    serviceRegistry = null;

    channel.shutdown();
    server.shutdown();

    try {
      channel.awaitTermination(1, TimeUnit.MINUTES);
      server.awaitTermination(1, TimeUnit.MINUTES);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      channel.shutdownNow();
      channel = null;

      server.shutdownNow();
      server = null;
    }
  }
}
