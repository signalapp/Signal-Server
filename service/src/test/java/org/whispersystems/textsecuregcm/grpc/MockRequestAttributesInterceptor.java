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
import java.net.InetAddress;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;

public class MockRequestAttributesInterceptor implements ServerInterceptor {

  @Nullable
  private InetAddress remoteAddress;

  @Nullable
  private UserAgent userAgent;

  @Nullable
  private List<Locale.LanguageRange> acceptLanguage;

  public void setRemoteAddress(@Nullable final InetAddress remoteAddress) {
    this.remoteAddress = remoteAddress;
  }

  public void setUserAgent(@Nullable final UserAgent userAgent) {
    this.userAgent = userAgent;
  }

  public void setAcceptLanguage(@Nullable final List<Locale.LanguageRange> acceptLanguage) {
    this.acceptLanguage = acceptLanguage;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    Context context = Context.current();

    if (remoteAddress != null) {
      context = context.withValue(RequestAttributesUtil.REMOTE_ADDRESS_CONTEXT_KEY, remoteAddress);
    }

    if (userAgent != null) {
      context = context.withValue(RequestAttributesUtil.USER_AGENT_CONTEXT_KEY, userAgent);
    }

    if (acceptLanguage != null) {
      context = context.withValue(RequestAttributesUtil.ACCEPT_LANGUAGE_CONTEXT_KEY, acceptLanguage);
    }

    return Contexts.interceptCall(context, serverCall, headers, next);
  }
}
