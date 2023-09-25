/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.whispersystems.textsecuregcm.util.HeaderUtils.basicCredentialsFromAuthHeader;

import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.basic.BasicCredentials;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.websocket.auth.AuthenticationException;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;


public class WebSocketAccountAuthenticator implements WebSocketAuthenticator<AuthenticatedAccount> {

  private static final AuthenticationResult<AuthenticatedAccount> CREDENTIALS_NOT_PRESENTED =
      new AuthenticationResult<>(Optional.empty(), false);

  private static final AuthenticationResult<AuthenticatedAccount> INVALID_CREDENTIALS_PRESENTED =
      new AuthenticationResult<>(Optional.empty(), true);

  private final AccountAuthenticator accountAuthenticator;


  public WebSocketAccountAuthenticator(final AccountAuthenticator accountAuthenticator) {
    this.accountAuthenticator = accountAuthenticator;
  }

  @Override
  public AuthenticationResult<AuthenticatedAccount> authenticate(final UpgradeRequest request)
      throws AuthenticationException {
    try {
      final AuthenticationResult<AuthenticatedAccount> authResultFromHeader =
          authenticatedAccountFromHeaderAuth(request.getHeader(HttpHeaders.AUTHORIZATION));
      // the logic here is that if the `Authorization` header was set for the request,
      // it takes the priority and we use the result of the header-based auth
      // ignoring the result of the query-based auth.
      if (authResultFromHeader.credentialsPresented()) {
        return authResultFromHeader;
      }
      return authenticatedAccountFromQueryParams(request);
    } catch (final Exception e) {
      // this will be handled and logged upstream
      // the most likely exception is a transient error connecting to account storage
      throw new AuthenticationException(e);
    }
  }

  private AuthenticationResult<AuthenticatedAccount> authenticatedAccountFromQueryParams(final UpgradeRequest request) {
    final Map<String, List<String>> parameters = request.getParameterMap();
    final List<String> usernames = parameters.get("login");
    final List<String> passwords = parameters.get("password");
    if (usernames == null || usernames.size() == 0 ||
        passwords == null || passwords.size() == 0) {
      return CREDENTIALS_NOT_PRESENTED;
    }
    final BasicCredentials credentials = new BasicCredentials(usernames.get(0).replace(" ", "+"),
        passwords.get(0).replace(" ", "+"));
    return new AuthenticationResult<>(accountAuthenticator.authenticate(credentials), true);
  }

  private AuthenticationResult<AuthenticatedAccount> authenticatedAccountFromHeaderAuth(@Nullable final String authHeader)
      throws AuthenticationException {
    if (authHeader == null) {
      return CREDENTIALS_NOT_PRESENTED;
    }
    return basicCredentialsFromAuthHeader(authHeader)
        .map(credentials -> new AuthenticationResult<>(accountAuthenticator.authenticate(credentials), true))
        .orElse(INVALID_CREDENTIALS_PRESENTED);
  }
}
