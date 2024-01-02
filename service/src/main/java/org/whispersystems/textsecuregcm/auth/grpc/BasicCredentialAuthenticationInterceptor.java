/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.grpc;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.basic.BasicCredentials;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.util.HeaderUtils;

/**
 * A basic credential authentication interceptor enforces the presence of a valid username and password on every call.
 * Callers supply credentials by providing a username (UUID and optional device ID) and password pair in the
 * {@code x-signal-basic-auth-credentials} call header.
 * <p/>
 * Downstream services can retrieve the identity of the authenticated caller using methods in
 * {@link AuthenticationUtil}.
 * <p/>
 * Note that this authentication, while fully functional, is intended only for development and testing purposes and is
 * intended to be replaced with a more robust and efficient strategy before widespread client adoption.
 *
 * @see AuthenticationUtil
 * @see AccountAuthenticator
 */
public class BasicCredentialAuthenticationInterceptor implements ServerInterceptor {

  private final AccountAuthenticator accountAuthenticator;

  @VisibleForTesting
  static final Metadata.Key<String> BASIC_CREDENTIALS =
      Metadata.Key.of("x-signal-auth", Metadata.ASCII_STRING_MARSHALLER);

  private static final Metadata EMPTY_TRAILERS = new Metadata();

  public BasicCredentialAuthenticationInterceptor(final AccountAuthenticator accountAuthenticator) {
    this.accountAuthenticator = accountAuthenticator;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    final String authHeader = headers.get(BASIC_CREDENTIALS);

    if (StringUtils.isNotBlank(authHeader)) {
      final Optional<BasicCredentials> maybeCredentials = HeaderUtils.basicCredentialsFromAuthHeader(authHeader);
      if (maybeCredentials.isEmpty()) {
        call.close(Status.UNAUTHENTICATED.withDescription("Could not parse credentials"), EMPTY_TRAILERS);
      } else {
        final Optional<AuthenticatedAccount> maybeAuthenticatedAccount =
            accountAuthenticator.authenticate(maybeCredentials.get());

        if (maybeAuthenticatedAccount.isPresent()) {
          final AuthenticatedAccount authenticatedAccount = maybeAuthenticatedAccount.get();

          final Context context = Context.current()
              .withValue(AuthenticationUtil.CONTEXT_AUTHENTICATED_ACCOUNT_IDENTIFIER_KEY, authenticatedAccount.getAccount().getUuid())
              .withValue(AuthenticationUtil.CONTEXT_AUTHENTICATED_DEVICE_IDENTIFIER_KEY, authenticatedAccount.getAuthenticatedDevice().getId());

          return Contexts.interceptCall(context, call, headers, next);
        } else {
          call.close(Status.UNAUTHENTICATED.withDescription("Credentials not accepted"), EMPTY_TRAILERS);
        }
      }
    } else {
      call.close(Status.UNAUTHENTICATED.withDescription("No credentials provided"), EMPTY_TRAILERS);
    }

    return new ServerCall.Listener<>() {};
  }
}
