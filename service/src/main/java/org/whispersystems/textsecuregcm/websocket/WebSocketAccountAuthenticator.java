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
import javax.annotation.Nullable;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.websocket.ReusableAuth;
import org.whispersystems.websocket.auth.AuthenticationException;
import org.whispersystems.websocket.auth.PrincipalSupplier;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;


public class WebSocketAccountAuthenticator implements WebSocketAuthenticator<AuthenticatedDevice> {

  private static final ReusableAuth<AuthenticatedDevice> CREDENTIALS_NOT_PRESENTED = ReusableAuth.anonymous();

  private static final ReusableAuth<AuthenticatedDevice> INVALID_CREDENTIALS_PRESENTED = ReusableAuth.invalid();

  private final AccountAuthenticator accountAuthenticator;
  private final PrincipalSupplier<AuthenticatedDevice> principalSupplier;

  public WebSocketAccountAuthenticator(final AccountAuthenticator accountAuthenticator,
      final PrincipalSupplier<AuthenticatedDevice> principalSupplier) {
    this.accountAuthenticator = accountAuthenticator;
    this.principalSupplier = principalSupplier;
  }

  @Override
  public ReusableAuth<AuthenticatedDevice> authenticate(final UpgradeRequest request)
      throws AuthenticationException {
    try {
      // If the `Authorization` header was set for the request it takes priority, and we use the result of the
      // header-based auth ignoring the result of the query-based auth.
      final String authHeader = request.getHeader(HttpHeaders.AUTHORIZATION);
      if (authHeader != null) {
        return authenticatedAccountFromHeaderAuth(authHeader);
      }
      return authenticatedAccountFromQueryParams(request);
    } catch (final Exception e) {
      // this will be handled and logged upstream
      // the most likely exception is a transient error connecting to account storage
      throw new AuthenticationException(e);
    }
  }

  private ReusableAuth<AuthenticatedDevice> authenticatedAccountFromQueryParams(final UpgradeRequest request) {
    final Map<String, List<String>> parameters = request.getParameterMap();
    final List<String> usernames = parameters.get("login");
    final List<String> passwords = parameters.get("password");
    if (usernames == null || usernames.size() == 0 ||
        passwords == null || passwords.size() == 0) {
      return CREDENTIALS_NOT_PRESENTED;
    }
    final BasicCredentials credentials = new BasicCredentials(usernames.get(0).replace(" ", "+"),
        passwords.get(0).replace(" ", "+"));
    return accountAuthenticator.authenticate(credentials)
        .map(authenticatedAccount -> ReusableAuth.authenticated(authenticatedAccount, this.principalSupplier))
        .orElse(INVALID_CREDENTIALS_PRESENTED);
  }

  private ReusableAuth<AuthenticatedDevice> authenticatedAccountFromHeaderAuth(@Nullable final String authHeader)
      throws AuthenticationException {
    if (authHeader == null) {
      return CREDENTIALS_NOT_PRESENTED;
    }
    return basicCredentialsFromAuthHeader(authHeader)
        .flatMap(credentials -> accountAuthenticator.authenticate(credentials))
        .map(authenticatedAccount -> ReusableAuth.authenticated(authenticatedAccount, this.principalSupplier))
        .orElse(INVALID_CREDENTIALS_PRESENTED);
  }
}
