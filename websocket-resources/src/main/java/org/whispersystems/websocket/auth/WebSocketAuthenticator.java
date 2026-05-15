/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.auth;

import java.security.Principal;
import java.util.Optional;
import org.eclipse.jetty.websocket.api.UpgradeRequest;

public interface WebSocketAuthenticator<T extends Principal> {

  /**
   * Authenticates an account from credential headers provided in a WebSocket upgrade request.
   *
   * @param request the request from which to extract credentials
   *
   * @return the authenticated principal if credentials were provided and authenticated or empty if the caller is
   * anonymous
   *
   * @throws InvalidCredentialsException if credentials were provided, but could not be authenticated
   */
  Optional<T> authenticate(UpgradeRequest request) throws InvalidCredentialsException;
}
