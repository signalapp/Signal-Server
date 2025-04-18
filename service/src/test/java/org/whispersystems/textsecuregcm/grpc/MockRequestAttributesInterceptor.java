/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.common.net.InetAddresses;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.net.InetAddress;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;

public class MockRequestAttributesInterceptor implements ServerInterceptor {

  private RequestAttributes requestAttributes = new RequestAttributes(InetAddresses.forString("127.0.0.1"), null, null);

  public void setRequestAttributes(final RequestAttributes requestAttributes) {
    this.requestAttributes = requestAttributes;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    return Contexts.interceptCall(Context.current()
        .withValue(RequestAttributesUtil.REQUEST_ATTRIBUTES_CONTEXT_KEY, requestAttributes), serverCall, headers, next);
  }
}
