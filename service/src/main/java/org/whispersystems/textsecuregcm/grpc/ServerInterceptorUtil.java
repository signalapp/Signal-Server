/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.Status;

public class ServerInterceptorUtil {

  @SuppressWarnings("rawtypes")
  private static final ServerCall.Listener NO_OP_LISTENER = new ServerCall.Listener<>() {};

  private static final Metadata EMPTY_TRAILERS = new Metadata();

  private ServerInterceptorUtil() {
  }

  /**
   * Closes the given server call with the given status, returning a no-op listener.
   *
   * @param call the server call to close
   * @param status the status with which to close the call
   *
   * @return a no-op server call listener
   *
   * @param <ReqT> the type of request object handled by the server call
   * @param <RespT> the type of response object returned by the server call
   */
  public static <ReqT, RespT> ServerCall.Listener<ReqT> closeWithStatus(final ServerCall<ReqT, RespT> call, final Status status) {
    call.close(status, EMPTY_TRAILERS);

    //noinspection unchecked
    return NO_OP_LISTENER;
  }
}
