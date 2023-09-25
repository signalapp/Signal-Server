/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.auth;

import java.security.Principal;
import java.util.Optional;
import org.eclipse.jetty.websocket.api.UpgradeRequest;

public interface WebSocketAuthenticator<T extends Principal> {
  AuthenticationResult<T> authenticate(UpgradeRequest request) throws AuthenticationException;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  class AuthenticationResult<T> {
    private final Optional<T> user;
    private final boolean credentialsPresented;

    public AuthenticationResult(final Optional<T> user, final boolean credentialsPresented) {
      this.user = user;
      this.credentialsPresented = credentialsPresented;
    }

    public Optional<T> getUser() {
      return user;
    }

    public boolean credentialsPresented() {
      return credentialsPresented;
    }
  }
}
