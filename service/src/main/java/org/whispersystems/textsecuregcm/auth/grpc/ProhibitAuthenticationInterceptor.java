package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.whispersystems.textsecuregcm.grpc.ChannelNotFoundException;
import org.whispersystems.textsecuregcm.grpc.ServerInterceptorUtil;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;

/**
 * A "prohibit authentication" interceptor ensures that requests to endpoints that should be invoked anonymously do not
 * originate from a channel that is associated with an authenticated device. Calls with an associated authenticated
 * device are closed with an {@code UNAUTHENTICATED} status. If a call's authentication status cannot be determined
 * (i.e. because the underlying remote channel closed before the {@code ServerCall} started), the interceptor will
 * reject the call with a status of {@code UNAVAILABLE}.
 */
public class ProhibitAuthenticationInterceptor extends AbstractAuthenticationInterceptor {

  public ProhibitAuthenticationInterceptor(final GrpcClientConnectionManager grpcClientConnectionManager) {
    super(grpcClientConnectionManager);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    try {
      return getAuthenticatedDevice(call)
          // Status.INTERNAL may seem a little surprising here, but if a caller is reaching an authentication-prohibited
          // service via an authenticated connection, then that's actually a server configuration issue and not a
          // problem with the client's request.
          .map(ignored -> ServerInterceptorUtil.closeWithStatus(call, Status.INTERNAL))
          .orElseGet(() -> next.startCall(call, headers));
    } catch (final ChannelNotFoundException e) {
      return ServerInterceptorUtil.closeWithStatus(call, Status.UNAVAILABLE);
    }
  }
}
