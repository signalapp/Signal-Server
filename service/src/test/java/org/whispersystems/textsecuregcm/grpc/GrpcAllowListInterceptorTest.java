/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoResponse;
import org.signal.chat.rpc.EchoServiceGrpc;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicGrpcAllowListConfiguration;
import org.whispersystems.textsecuregcm.tests.util.FakeDynamicConfigurationManager;


class GrpcAllowListInterceptorTest {
  private Server server;
  private ManagedChannel channel;

  @BeforeEach
  void setUp() {
    channel = InProcessChannelBuilder.forName("GrpcAllowListInterceptorTest")
        .directExecutor()
        .build();
  }

  @AfterEach
  void tearDown() throws Exception {
    server.shutdownNow();
    channel.shutdownNow();
    server.awaitTermination(1, TimeUnit.SECONDS);
    channel.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void disableAll() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(false, Collections.emptySet(), Collections.emptySet());
    GrpcTestUtils.assertStatusException(Status.UNIMPLEMENTED, () ->
        client.echo(EchoRequest.newBuilder().setPayload(ByteString.empty()).build()));
  }

  @Test
  public void enableAll() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(true, Collections.emptySet(), Collections.emptySet());
    final EchoResponse echo = client.echo(EchoRequest.newBuilder().setPayload(ByteString.empty()).build());
    assertThat(echo.getPayload()).isEqualTo(ByteString.empty());
  }

  @Test
  public void enableByMethod() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(false, Collections.emptySet(), Set.of("org.signal.chat.rpc.EchoService/echo"));

    final EchoResponse echo = client.echo(EchoRequest.newBuilder().setPayload(ByteString.empty()).build());
    assertThat(echo.getPayload()).isEqualTo(ByteString.empty());

    GrpcTestUtils.assertStatusException(Status.UNIMPLEMENTED, () ->
        client.echo2(EchoRequest.newBuilder().setPayload(ByteString.empty()).build()));
  }

  @Test
  public void enableByService() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(false, Set.of("org.signal.chat.rpc.EchoService"), Collections.emptySet());

    final EchoResponse echo = client.echo(EchoRequest.newBuilder().setPayload(ByteString.empty()).build());
    assertThat(echo.getPayload()).isEqualTo(ByteString.empty());

    final EchoResponse echo2 = client.echo2(EchoRequest.newBuilder().setPayload(ByteString.empty()).build());
    assertThat(echo2.getPayload()).isEqualTo(ByteString.empty());
  }

  @Test
  public void enableByServiceWrongService() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(false, Set.of("org.signal.chat.rpc.NotEchoService"), Collections.emptySet());

    GrpcTestUtils.assertStatusException(Status.UNIMPLEMENTED, () ->
        client.echo(EchoRequest.newBuilder().setPayload(ByteString.empty()).build()));
  }

  private EchoServiceGrpc.EchoServiceBlockingStub setup(
      boolean enableAll,
      Set<String> enabledServices,
      Set<String> enabledMethods)
      throws IOException {
    if (server != null) {
      server.shutdownNow();
    }
    final DynamicConfiguration configuration = mock(DynamicConfiguration.class);
    when(configuration.getGrpcAllowList())
        .thenReturn(new DynamicGrpcAllowListConfiguration(enableAll, enabledServices, enabledMethods));
    server = InProcessServerBuilder.forName("GrpcAllowListInterceptorTest")
        .directExecutor()
        .addService(new EchoServiceImpl())
        .intercept(new GrpcAllowListInterceptor(new FakeDynamicConfigurationManager<>(configuration)))
        .build()
        .start();

    return EchoServiceGrpc.newBlockingStub(channel);
  }
}
