/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoResponse;
import org.signal.chat.rpc.EchoServiceGrpc;

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
        setup(false, Collections.emptyList(), Collections.emptyList());
    GrpcTestUtils.assertStatusException(Status.UNIMPLEMENTED, () ->
        client.echo(EchoRequest.newBuilder().setPayload(ByteString.empty()).build()));
  }

  @Test
  public void enableAll() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(true, Collections.emptyList(), Collections.emptyList());
    final EchoResponse echo = client.echo(EchoRequest.newBuilder().setPayload(ByteString.empty()).build());
    assertThat(echo.getPayload()).isEqualTo(ByteString.empty());
  }

  @Test
  public void enableByMethod() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(false, Collections.emptyList(), List.of("org.signal.chat.rpc.EchoService/echo"));

    final EchoResponse echo = client.echo(EchoRequest.newBuilder().setPayload(ByteString.empty()).build());
    assertThat(echo.getPayload()).isEqualTo(ByteString.empty());

    GrpcTestUtils.assertStatusException(Status.UNIMPLEMENTED, () ->
        client.echo2(EchoRequest.newBuilder().setPayload(ByteString.empty()).build()));
  }

  @Test
  public void enableByService() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(false, List.of("org.signal.chat.rpc.EchoService"), Collections.emptyList());

    final EchoResponse echo = client.echo(EchoRequest.newBuilder().setPayload(ByteString.empty()).build());
    assertThat(echo.getPayload()).isEqualTo(ByteString.empty());

    final EchoResponse echo2 = client.echo2(EchoRequest.newBuilder().setPayload(ByteString.empty()).build());
    assertThat(echo2.getPayload()).isEqualTo(ByteString.empty());
  }

  @Test
  public void enableByServiceWrongService() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(false, List.of("org.signal.chat.rpc.NotEchoService"), Collections.emptyList());

    GrpcTestUtils.assertStatusException(Status.UNIMPLEMENTED, () ->
        client.echo(EchoRequest.newBuilder().setPayload(ByteString.empty()).build()));
  }

  private EchoServiceGrpc.EchoServiceBlockingStub setup(
      boolean enableAll,
      List<String> enabledServices,
      List<String> enabledMethods)
      throws IOException {
    if (server != null) {
      server.shutdownNow();
    }
    server = InProcessServerBuilder.forName("GrpcAllowListInterceptorTest")
        .directExecutor()
        .addService(new EchoServiceImpl())
        .intercept(new GrpcAllowListInterceptor(enableAll, enabledServices, enabledMethods))
        .build()
        .start();

    return EchoServiceGrpc.newBlockingStub(channel);
  }
}
