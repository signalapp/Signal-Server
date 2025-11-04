/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.whispersystems.textsecuregcm.util.HeaderUtils.basicCredentialsFromAuthHeader;

import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.basic.BasicCredentials;
import java.util.Optional;
import javax.annotation.Nullable;
import org.eclipse.jetty.ee10.websocket.server.JettyServerUpgradeRequest;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.websocket.auth.InvalidCredentialsException;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;


public class WebSocketAccountAuthenticator implements WebSocketAuthenticator<AuthenticatedDevice> {

  private final AccountAuthenticator accountAuthenticator;

  public WebSocketAccountAuthenticator(final AccountAuthenticator accountAuthenticator) {
    this.accountAuthenticator = accountAuthenticator;
  }

  @Override
  public Optional<AuthenticatedDevice> authenticate(final JettyServerUpgradeRequest request)
      throws InvalidCredentialsException {

    @Nullable final String authHeader = request.getHeader(HttpHeaders.AUTHORIZATION);

    if (authHeader == null) {
      return Optional.empty();
    }

    final BasicCredentials credentials = basicCredentialsFromAuthHeader(authHeader)
        .orElseThrow(InvalidCredentialsException::new);

    final AuthenticatedDevice authenticatedDevice = accountAuthenticator.authenticate(credentials)
        .orElseThrow(InvalidCredentialsException::new);

    return Optional.of(authenticatedDevice);
  }
}
