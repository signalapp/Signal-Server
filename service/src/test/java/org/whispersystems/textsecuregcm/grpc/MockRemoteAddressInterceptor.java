/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.net.SocketAddress;
import javax.annotation.Nullable;

public class MockRemoteAddressInterceptor implements ServerInterceptor {

  @Nullable
  private SocketAddress remoteAddress;

  public void setRemoteAddress(@Nullable final SocketAddress remoteAddress) {
    this.remoteAddress = remoteAddress;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    return remoteAddress == null
        ? next.startCall(serverCall, headers)
        : Contexts.interceptCall(
            Context.current().withValue(RemoteAddressUtil.REMOTE_ADDRESS_CONTEXT_KEY, remoteAddress),
            serverCall, headers, next);
  }
}
