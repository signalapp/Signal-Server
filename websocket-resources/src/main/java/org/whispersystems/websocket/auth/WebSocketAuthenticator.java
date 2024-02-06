/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.auth;

import java.security.Principal;
import java.util.Optional;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.whispersystems.websocket.ReusableAuth;

public interface WebSocketAuthenticator<T extends Principal> {
  ReusableAuth<T> authenticate(UpgradeRequest request) throws AuthenticationException;
}
