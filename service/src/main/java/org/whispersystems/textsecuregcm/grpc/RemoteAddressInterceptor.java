/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.net.SocketAddress;

public class RemoteAddressInterceptor implements ServerInterceptor {

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    // Note: the specific implementation for getting a remote client address may change depending on the client
    // connection strategy. The important thing is that the remote address wind up in the context for the current
    // call so it can be retrieved by `RemoteAddressUtil`.
    final SocketAddress remoteAddress = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);

    return Contexts.interceptCall(
        Context.current().withValue(RemoteAddressUtil.REMOTE_ADDRESS_CONTEXT_KEY, remoteAddress),
        serverCall, headers, next);
  }
}
