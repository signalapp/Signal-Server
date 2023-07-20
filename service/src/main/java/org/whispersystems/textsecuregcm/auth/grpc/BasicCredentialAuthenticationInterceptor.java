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
import org.whispersystems.textsecuregcm.auth.BaseAccountAuthenticator;

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
 * @see BaseAccountAuthenticator
 */
public class BasicCredentialAuthenticationInterceptor implements ServerInterceptor {

  private final BaseAccountAuthenticator baseAccountAuthenticator;

  @VisibleForTesting
  static final Metadata.Key<String> BASIC_CREDENTIALS =
      Metadata.Key.of("x-signal-basic-auth-credentials", Metadata.ASCII_STRING_MARSHALLER);

  private static final Metadata EMPTY_TRAILERS = new Metadata();

  public BasicCredentialAuthenticationInterceptor(final BaseAccountAuthenticator baseAccountAuthenticator) {
    this.baseAccountAuthenticator = baseAccountAuthenticator;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    final String credentialString = headers.get(BASIC_CREDENTIALS);

    if (StringUtils.isNotBlank(credentialString)) {
      try {
        final BasicCredentials credentials = extractBasicCredentials(credentialString);
        final Optional<AuthenticatedAccount> maybeAuthenticatedAccount =
            baseAccountAuthenticator.authenticate(credentials, false);

        if (maybeAuthenticatedAccount.isPresent()) {
          final AuthenticatedAccount authenticatedAccount = maybeAuthenticatedAccount.get();

          final Context context = Context.current()
              .withValue(AuthenticationUtil.CONTEXT_AUTHENTICATED_ACCOUNT_IDENTIFIER_KEY, authenticatedAccount.getAccount().getUuid())
              .withValue(AuthenticationUtil.CONTEXT_AUTHENTICATED_DEVICE_IDENTIFIER_KEY, authenticatedAccount.getAuthenticatedDevice().getId());

          return Contexts.interceptCall(context, call, headers, next);
        } else {
          call.close(Status.UNAUTHENTICATED.withDescription("Credentials not accepted"), EMPTY_TRAILERS);
        }
      } catch (final IllegalArgumentException e) {
        call.close(Status.UNAUTHENTICATED.withDescription("Could not parse credentials"), EMPTY_TRAILERS);
      }
    } else {
      call.close(Status.UNAUTHENTICATED.withDescription("No credentials provided"), EMPTY_TRAILERS);
    }

    return new ServerCall.Listener<>() {};
  }

  @VisibleForTesting
  static BasicCredentials extractBasicCredentials(final String credentials) {
    if (credentials.indexOf(':') < 0) {
      throw new IllegalArgumentException("Credentials do not include a username and password part");
    }

    final String[] pieces = credentials.split(":", 2);

    return new BasicCredentials(pieces[0], pieces[1]);
  }
}
