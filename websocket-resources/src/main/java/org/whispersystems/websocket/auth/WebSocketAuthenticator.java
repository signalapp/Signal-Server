/*
 * Copyright (C) 2014 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.websocket.auth;

import org.eclipse.jetty.server.Authentication;
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
