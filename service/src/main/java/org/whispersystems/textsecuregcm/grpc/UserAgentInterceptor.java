/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;

import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

public class UserAgentInterceptor implements ServerInterceptor {
  @VisibleForTesting
  public static final Metadata.Key<String> USER_AGENT_GRPC_HEADER =
      Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    UserAgent userAgent;
    try {
      userAgent = UserAgentUtil.parseUserAgentString(headers.get(USER_AGENT_GRPC_HEADER));
    } catch (final UnrecognizedUserAgentException e) {
      userAgent = null;
    }

    final Context context = Context.current().withValue(UserAgentUtil.USER_AGENT_CONTEXT_KEY, userAgent);
    return Contexts.interceptCall(context, call, headers, next);
  }

}
