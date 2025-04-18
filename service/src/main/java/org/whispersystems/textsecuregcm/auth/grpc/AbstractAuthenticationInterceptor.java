package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.ServerCall;
import io.grpc.ServerInterceptor;
import java.util.Optional;
import org.whispersystems.textsecuregcm.grpc.ChannelNotFoundException;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;

abstract class AbstractAuthenticationInterceptor implements ServerInterceptor {

  private final GrpcClientConnectionManager grpcClientConnectionManager;

  AbstractAuthenticationInterceptor(final GrpcClientConnectionManager grpcClientConnectionManager) {
    this.grpcClientConnectionManager = grpcClientConnectionManager;
  }

  protected Optional<AuthenticatedDevice> getAuthenticatedDevice(final ServerCall<?, ?> call)
      throws ChannelNotFoundException {

    return grpcClientConnectionManager.getAuthenticatedDevice(call);
  }
}
