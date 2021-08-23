/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;

/**
 * Delegates request events to a listener that handles auth-enablement changes
 */
public class AuthEnablementApplicationEventListener implements ApplicationEventListener {

  private final AuthEnablementRequestEventListener authEnablementRequestEventListener;

  public AuthEnablementApplicationEventListener(final ClientPresenceManager clientPresenceManager) {
    this.authEnablementRequestEventListener = new AuthEnablementRequestEventListener(clientPresenceManager);
  }

  @Override
  public void onEvent(final ApplicationEvent event) {
  }

  @Override
  public RequestEventListener onRequest(final RequestEvent requestEvent) {
    return authEnablementRequestEventListener;
  }
}
