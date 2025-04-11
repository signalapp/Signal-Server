/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Context;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.netty.channel.local.LocalAddress;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;

/**
 * Then channel shutdown interceptor rejects new requests if a channel is shutting down and works in tandem with
 * {@link GrpcClientConnectionManager} to maintain an active call count for each channel otherwise.
 */
public class ChannelShutdownInterceptor implements ServerInterceptor {

  private final GrpcClientConnectionManager grpcClientConnectionManager;

  public ChannelShutdownInterceptor(final GrpcClientConnectionManager grpcClientConnectionManager) {
    this.grpcClientConnectionManager = grpcClientConnectionManager;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    if (!grpcClientConnectionManager.handleServerCallStart(call)) {
      // Don't allow new calls if the connection is getting ready to close
      return ServerInterceptorUtil.closeWithStatus(call, Status.UNAVAILABLE);
    }

    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
      @Override
      public void onComplete() {
        grpcClientConnectionManager.handleServerCallComplete(call);
        super.onComplete();
      }

      @Override
      public void onCancel() {
        grpcClientConnectionManager.handleServerCallComplete(call);
        super.onCancel();
      }
    };
  }
}
