package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;

/**
 * The request attributes interceptor makes request attributes from the underlying remote channel available to service
 * implementations by attaching them to a {@link Context} attribute that can be read via {@link RequestAttributesUtil}.
 * All server calls should have request attributes, and calls will be rejected with a status of {@code UNAVAILABLE} if
 * request attributes are unavailable (i.e. the underlying channel closed before the {@code ServerCall} started).
 *
 * @see RequestAttributesUtil
 */
public class RequestAttributesInterceptor implements ServerInterceptor {

  private final GrpcClientConnectionManager grpcClientConnectionManager;

  public RequestAttributesInterceptor(final GrpcClientConnectionManager grpcClientConnectionManager) {
    this.grpcClientConnectionManager = grpcClientConnectionManager;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    try {
      return Contexts.interceptCall(Context.current()
              .withValue(RequestAttributesUtil.REQUEST_ATTRIBUTES_CONTEXT_KEY,
                  grpcClientConnectionManager.getRequestAttributes(call)), call, headers, next);
    } catch (final ChannelNotFoundException e) {
      return ServerInterceptorUtil.closeWithStatus(call, Status.UNAVAILABLE);
    }
  }
}
