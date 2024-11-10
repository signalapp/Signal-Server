package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;

/**
 * A "require authentication" interceptor requires that requests be issued from a connection that is associated with an
 * authenticated device. Calls without an associated authenticated device are closed with an {@code UNAUTHENTICATED}
 * status.
 */
public class RequireAuthenticationInterceptor extends AbstractAuthenticationInterceptor {

  public RequireAuthenticationInterceptor(final GrpcClientConnectionManager grpcClientConnectionManager) {
    super(grpcClientConnectionManager);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    return getAuthenticatedDevice(call)
        .map(authenticatedDevice -> Contexts.interceptCall(Context.current()
                .withValue(AuthenticationUtil.CONTEXT_AUTHENTICATED_DEVICE, authenticatedDevice),
            call, headers, next))
        .orElseGet(() -> closeAsUnauthenticated(call));
  }
}
