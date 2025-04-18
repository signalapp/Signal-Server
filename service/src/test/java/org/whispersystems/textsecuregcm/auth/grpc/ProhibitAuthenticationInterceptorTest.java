package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.signal.chat.rpc.GetAuthenticatedDeviceResponse;
import org.whispersystems.textsecuregcm.grpc.ChannelNotFoundException;
import org.whispersystems.textsecuregcm.grpc.GrpcTestUtils;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;
import org.whispersystems.textsecuregcm.storage.Device;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class ProhibitAuthenticationInterceptorTest extends AbstractAuthenticationInterceptorTest {

  @Override
  protected AbstractAuthenticationInterceptor getInterceptor() {
    return new ProhibitAuthenticationInterceptor(getClientConnectionManager());
  }

  @Test
  void interceptCall() throws ChannelNotFoundException {
    final GrpcClientConnectionManager grpcClientConnectionManager = getClientConnectionManager();

    when(grpcClientConnectionManager.getAuthenticatedDevice(any())).thenReturn(Optional.empty());

    final GetAuthenticatedDeviceResponse response = getAuthenticatedDevice();
    assertTrue(response.getAccountIdentifier().isEmpty());
    assertEquals(0, response.getDeviceId());

    final AuthenticatedDevice authenticatedDevice = new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID);
    when(grpcClientConnectionManager.getAuthenticatedDevice(any())).thenReturn(Optional.of(authenticatedDevice));

    GrpcTestUtils.assertStatusException(Status.INTERNAL, this::getAuthenticatedDevice);

    when(grpcClientConnectionManager.getAuthenticatedDevice(any())).thenThrow(ChannelNotFoundException.class);

    GrpcTestUtils.assertStatusException(Status.UNAVAILABLE, this::getAuthenticatedDevice);
  }
}
