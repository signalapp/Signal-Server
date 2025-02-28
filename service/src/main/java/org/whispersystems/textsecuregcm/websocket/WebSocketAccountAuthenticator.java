/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.whispersystems.textsecuregcm.util.HeaderUtils.basicCredentialsFromAuthHeader;

import com.google.common.net.HttpHeaders;
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
      return authenticatedAccountFromHeaderAuth(request.getHeader(HttpHeaders.AUTHORIZATION));
    } catch (final Exception e) {
      // this will be handled and logged upstream
      // the most likely exception is a transient error connecting to account storage
      throw new AuthenticationException(e);
    }
  }

  private ReusableAuth<AuthenticatedDevice> authenticatedAccountFromHeaderAuth(@Nullable final String authHeader) {
    if (authHeader == null) {
      return CREDENTIALS_NOT_PRESENTED;
    }
    return basicCredentialsFromAuthHeader(authHeader)
        .flatMap(accountAuthenticator::authenticate)
        .map(authenticatedAccount -> ReusableAuth.authenticated(authenticatedAccount, this.principalSupplier))
        .orElse(INVALID_CREDENTIALS_PRESENTED);
  }
}
