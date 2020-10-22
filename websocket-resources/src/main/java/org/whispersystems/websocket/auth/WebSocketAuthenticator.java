/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.auth;

import org.eclipse.jetty.websocket.api.UpgradeRequest;

import java.security.Principal;
import java.util.Optional;

public interface WebSocketAuthenticator<T extends Principal> {
  AuthenticationResult<T> authenticate(UpgradeRequest request) throws AuthenticationException;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public class AuthenticationResult<T> {
    private final Optional<T> user;
    private final boolean     required;

    public AuthenticationResult(Optional<T> user, boolean required) {
      this.user     = user;
      this.required = required;
    }

    public Optional<T> getUser() {
      return user;
    }

    public boolean isRequired() {
      return required;
    }
  }
}
