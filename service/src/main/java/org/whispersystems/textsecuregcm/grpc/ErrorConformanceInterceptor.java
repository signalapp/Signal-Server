/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorConformanceInterceptor implements ServerInterceptor {

  private static final Logger log = LoggerFactory.getLogger(ErrorConformanceInterceptor.class);

  private static final Metadata.Key<byte[]> DETAILS_HEADER_KEY =
      Metadata.Key.of("grpc-status-details-bin", Metadata.BINARY_BYTE_MARSHALLER);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {
    return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
      @Override
      public void close(final Status status, final Metadata trailers) {
        if (status.getCode() == Status.Code.OK) {
          super.close(status, trailers);
          return;
        }
        if (!trailers.containsKey(DETAILS_HEADER_KEY)) {
          log.error("Intercepted call {} returned status {} but did not include status details",
              call.getMethodDescriptor().getFullMethodName(), status);
          assert false;
        }

        switch (status.getCode()) {
          case UNAUTHENTICATED, UNAVAILABLE, INVALID_ARGUMENT, RESOURCE_EXHAUSTED -> {
          }
          default -> {
            log.error("Intercepted call {} returned illegal application status {}: {}",
                call.getMethodDescriptor().getFullMethodName(), status, status.getDescription());
            assert false;
          }
        }
        super.close(status, trailers);
      }
    }, headers);
  }
}
