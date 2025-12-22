package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoServiceGrpc;
import org.whispersystems.textsecuregcm.grpc.EchoServiceImpl;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProhibitAuthenticationInterceptorTest  {
  private Server server;
  private ManagedChannel channel;

  @BeforeEach
  void setUp() throws Exception {
    server = InProcessServerBuilder.forName("RequestAttributesInterceptorTest")
        .directExecutor()
        .intercept(new ProhibitAuthenticationInterceptor())
        .addService(new EchoServiceImpl())
        .build()
        .start();

    channel = InProcessChannelBuilder.forName("RequestAttributesInterceptorTest")
        .directExecutor()
        .build();
  }

  @AfterEach
  void tearDown() throws Exception {
    channel.shutdownNow();
    server.shutdownNow();
    channel.awaitTermination(5, TimeUnit.SECONDS);
    server.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  void hasAuth() {
    final EchoServiceGrpc.EchoServiceBlockingStub client = EchoServiceGrpc
        .newBlockingStub(channel)
        .withCallCredentials(new BasicAuthCallCredentials("test", "password"));

    final StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
        () -> client.echo(EchoRequest.getDefaultInstance()));
    assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
  }

  @Test
  void noAuth() {
    final EchoServiceGrpc.EchoServiceBlockingStub client = EchoServiceGrpc.newBlockingStub(channel);
    assertDoesNotThrow(() -> client.echo(EchoRequest.getDefaultInstance()));
  }
}
