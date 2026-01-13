package org.whispersystems.textsecuregcm.auth.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.basic.BasicCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.chat.rpc.GetAuthenticatedDeviceRequest;
import org.signal.chat.rpc.GetAuthenticatedDeviceResponse;
import org.signal.chat.rpc.GetRequestAttributesRequest;
import org.signal.chat.rpc.RequestAttributesGrpc;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.grpc.RequestAttributesServiceImpl;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class RequireAuthenticationInterceptorTest {
  private Server server;
  private ManagedChannel channel;
  private AccountAuthenticator authenticator;

  @BeforeEach
  void setUp() throws Exception {
    authenticator = mock(AccountAuthenticator.class);
    server = InProcessServerBuilder.forName("RequestAttributesInterceptorTest")
        .directExecutor()
        .intercept(new RequireAuthenticationInterceptor(authenticator))
        .addService(new RequestAttributesServiceImpl())
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
    final UUID aci = UUID.randomUUID();
    final byte deviceId = 2;
    when(authenticator.authenticate(eq(new BasicCredentials("test", "password"))))
        .thenReturn(Optional.of(
            new org.whispersystems.textsecuregcm.auth.AuthenticatedDevice(aci, deviceId, Instant.now())));

    final RequestAttributesGrpc.RequestAttributesBlockingStub client = RequestAttributesGrpc
        .newBlockingStub(channel)
        .withCallCredentials(new BasicAuthCallCredentials("test", "password"));

    final GetAuthenticatedDeviceResponse authenticatedDevice = client.getAuthenticatedDevice(
        GetAuthenticatedDeviceRequest.getDefaultInstance());
    assertEquals(deviceId, authenticatedDevice.getDeviceId());
    assertEquals(UUIDUtil.fromByteString(authenticatedDevice.getAccountIdentifier()), aci);
  }

  @Test
  void badCredentials() {
    when(authenticator.authenticate(any())).thenReturn(Optional.empty());

    final RequestAttributesGrpc.RequestAttributesBlockingStub client = RequestAttributesGrpc
        .newBlockingStub(channel)
        .withCallCredentials(new BasicAuthCallCredentials("test", "password"));

    final StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
        () -> client.getRequestAttributes(GetRequestAttributesRequest.getDefaultInstance()));
    assertEquals(Status.Code.UNAUTHENTICATED, e.getStatus().getCode());
  }

  @Test
  void missingCredentials() {
    when(authenticator.authenticate(any())).thenReturn(Optional.empty());

    final RequestAttributesGrpc.RequestAttributesBlockingStub client = RequestAttributesGrpc.newBlockingStub(channel);

    final StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
        () -> client.getRequestAttributes(GetRequestAttributesRequest.getDefaultInstance()));
    assertEquals(Status.Code.UNAUTHENTICATED, e.getStatus().getCode());
  }
}
