package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.whispersystems.textsecuregcm.grpc.ChannelNotFoundException;
import org.whispersystems.textsecuregcm.grpc.ServerInterceptorUtil;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;

/**
 * A "require authentication" interceptor requires that requests be issued from a connection that is associated with an
 * authenticated device. Calls without an associated authenticated device are closed with an {@code UNAUTHENTICATED}
 * status. If a call's authentication status cannot be determined (i.e. because the underlying remote channel closed
 * before the {@code ServerCall} started), the interceptor will reject the call with a status of {@code UNAVAILABLE}.
 */
public class RequireAuthenticationInterceptor extends AbstractAuthenticationInterceptor {

  public RequireAuthenticationInterceptor(final GrpcClientConnectionManager grpcClientConnectionManager) {
    super(grpcClientConnectionManager);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    try {
      return getAuthenticatedDevice(call)
          .map(authenticatedDevice -> Contexts.interceptCall(Context.current()
                  .withValue(AuthenticationUtil.CONTEXT_AUTHENTICATED_DEVICE, authenticatedDevice),
              call, headers, next))
          // Status.INTERNAL may seem a little surprising here, but if a caller is reaching an authentication-required
          // service via an unauthenticated connection, then that's actually a server configuration issue and not a
          // problem with the client's request.
          .orElseGet(() -> ServerInterceptorUtil.closeWithStatus(call, Status.INTERNAL));
    } catch (final ChannelNotFoundException e) {
      return ServerInterceptorUtil.closeWithStatus(call, Status.UNAVAILABLE);
    }
  }
}
