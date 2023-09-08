/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * This interceptor observes responses from the service and if the response status is {@link Status#UNKNOWN}
 * and there is a non-null cause which is an instance of {@link ConvertibleToGrpcStatus},
 * then status and metadata to be returned to the client is resolved from that object.
 * </p>
 * This eliminates the need of having each service to override {@code `onErrorMap()`} method for commonly used exceptions.
 */
public class ErrorMappingInterceptor implements ServerInterceptor {

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {
    return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
      @Override
      public void close(final Status status, final Metadata trailers) {
        // The idea is to only apply the automatic conversion logic in the cases
        // when there was no explicit decision by the service to provide a status.
        // I.e. if at this point we see anything but the `UNKNOWN`,
        // that means that some logic in the service made this decision already
        // and automatic conversion may conflict with it.
        if (status.getCode().equals(Status.Code.UNKNOWN)
            && status.getCause() instanceof ConvertibleToGrpcStatus convertibleToGrpcStatus) {
          super.close(
              convertibleToGrpcStatus.grpcStatus(),
              convertibleToGrpcStatus.grpcMetadata().orElseGet(Metadata::new)
          );
        } else {
          super.close(status, trailers);
        }
      }
    }, headers);
  }
}
