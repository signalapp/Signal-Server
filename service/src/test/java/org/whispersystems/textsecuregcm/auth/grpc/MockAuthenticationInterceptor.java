/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.UUID;
import javax.annotation.Nullable;

public class MockAuthenticationInterceptor implements ServerInterceptor {

  @Nullable
  private AuthenticatedDevice authenticatedDevice;

  public void setAuthenticatedDevice(final UUID accountIdentifier, final byte deviceId) {
    authenticatedDevice = new AuthenticatedDevice(accountIdentifier, deviceId);
  }

  public void clearAuthenticatedDevice() {
    authenticatedDevice = null;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    return authenticatedDevice != null
        ? Contexts.interceptCall(
        Context.current().withValue(AuthenticationUtil.CONTEXT_AUTHENTICATED_DEVICE, authenticatedDevice),
        call, headers, next)
        : next.startCall(call, headers);
  }
}
