package org.whispersystems.textsecuregcm.auth.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.signal.chat.rpc.GetAuthenticatedDeviceResponse;
import org.whispersystems.textsecuregcm.grpc.ChannelNotFoundException;
import org.whispersystems.textsecuregcm.grpc.GrpcTestUtils;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class RequireAuthenticationInterceptorTest extends AbstractAuthenticationInterceptorTest {

  @Override
  protected AbstractAuthenticationInterceptor getInterceptor() {
    return new RequireAuthenticationInterceptor(getClientConnectionManager());
  }

  @Test
  void interceptCall() throws ChannelNotFoundException {
    final GrpcClientConnectionManager grpcClientConnectionManager = getClientConnectionManager();

    when(grpcClientConnectionManager.getAuthenticatedDevice(any())).thenReturn(Optional.empty());

    GrpcTestUtils.assertStatusException(Status.INTERNAL, this::getAuthenticatedDevice);

    final AuthenticatedDevice authenticatedDevice = new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID);
    when(grpcClientConnectionManager.getAuthenticatedDevice(any())).thenReturn(Optional.of(authenticatedDevice));

    final GetAuthenticatedDeviceResponse response = getAuthenticatedDevice();
    assertEquals(UUIDUtil.toByteString(authenticatedDevice.accountIdentifier()), response.getAccountIdentifier());
    assertEquals(authenticatedDevice.deviceId(), response.getDeviceId());

    when(grpcClientConnectionManager.getAuthenticatedDevice(any())).thenThrow(ChannelNotFoundException.class);

    GrpcTestUtils.assertStatusException(Status.UNAVAILABLE, this::getAuthenticatedDevice);
  }
}
