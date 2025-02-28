/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.google.common.annotations.VisibleForTesting;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeRequest;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeResponse;
import org.whispersystems.websocket.ReusableAuth;
import org.whispersystems.websocket.auth.AuthenticatedWebSocketUpgradeFilter;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

public class IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter implements
    AuthenticatedWebSocketUpgradeFilter<AuthenticatedDevice> {

  private final Duration minIdleDuration;
  private final Clock clock;

  @VisibleForTesting
  static final String ALERT_HEADER = "X-Signal-Alert";

  @VisibleForTesting
  static final String IDLE_PRIMARY_DEVICE_ALERT = "idle-primary-device";

  public IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter(final Duration minIdleDuration, final Clock clock) {
    this.minIdleDuration = minIdleDuration;
    this.clock = clock;
  }

  @Override
  public void handleAuthentication(final ReusableAuth<AuthenticatedDevice> authenticated,
      final JettyServerUpgradeRequest request,
      final JettyServerUpgradeResponse response) {

    // No action needed if the connection is unauthenticated (in which case we don't know when we've last seen the
    // primary device) or if the authenticated device IS the primary device
    authenticated.ref()
        .filter(authenticatedDevice -> !authenticatedDevice.getAuthenticatedDevice().isPrimary())
        .ifPresent(authenticatedDevice -> {
          final Instant primaryDeviceLastSeen =
              Instant.ofEpochMilli(authenticatedDevice.getAccount().getPrimaryDevice().getLastSeen());

          if (primaryDeviceLastSeen.isBefore(clock.instant().minus(minIdleDuration))) {
            response.addHeader(ALERT_HEADER, IDLE_PRIMARY_DEVICE_ALERT);
          }
        });
  }
}
