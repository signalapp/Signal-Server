package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.netty.channel.local.LocalAddress;
import org.whispersystems.textsecuregcm.grpc.net.ClientConnectionManager;
import java.util.Optional;

abstract class AbstractAuthenticationInterceptor implements ServerInterceptor {

  private final ClientConnectionManager clientConnectionManager;

  private static final Metadata EMPTY_TRAILERS = new Metadata();

  AbstractAuthenticationInterceptor(final ClientConnectionManager clientConnectionManager) {
    this.clientConnectionManager = clientConnectionManager;
  }

  protected Optional<AuthenticatedDevice> getAuthenticatedDevice(final ServerCall<?, ?> call) {
    if (call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR) instanceof LocalAddress localAddress) {
      return clientConnectionManager.getAuthenticatedDevice(localAddress);
    } else {
      throw new AssertionError("Unexpected channel type: " + call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
    }
  }

  protected <ReqT, RespT> ServerCall.Listener<ReqT> closeAsUnauthenticated(final ServerCall<ReqT, RespT> call) {
    call.close(Status.UNAUTHENTICATED, EMPTY_TRAILERS);
    return new ServerCall.Listener<>() {};
  }
}
