package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;

/**
 * A "prohibit authentication" interceptor ensures that requests to endpoints that should be invoked anonymously do not
 * originate from a channel that is associated with an authenticated device. Calls with an associated authenticated
 * device are closed with an {@code UNAUTHENTICATED} status.
 */
public class ProhibitAuthenticationInterceptor extends AbstractAuthenticationInterceptor {

  public ProhibitAuthenticationInterceptor(final GrpcClientConnectionManager grpcClientConnectionManager) {
    super(grpcClientConnectionManager);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    return getAuthenticatedDevice(call)
        .map(ignored -> closeAsUnauthenticated(call))
        .orElseGet(() -> next.startCall(call, headers));
  }
}
