/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.websocket.auth;

import java.security.Principal;
import java.util.Optional;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeRequest;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeResponse;

public interface AuthenticatedWebSocketUpgradeFilter<T extends Principal> {

  void handleAuthentication(@SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<T> authenticated,
      JettyServerUpgradeRequest request,
      JettyServerUpgradeResponse response);
}
