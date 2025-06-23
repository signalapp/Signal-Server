/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeRequest;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeResponse;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.websocket.auth.AuthenticatedWebSocketUpgradeFilter;

public class IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter implements
    AuthenticatedWebSocketUpgradeFilter<AuthenticatedDevice> {

  private final KeysManager keysManager;

  private final Duration minIdleDuration;
  private final Clock clock;

  @VisibleForTesting
  static final String ALERT_HEADER = "X-Signal-Alert";

  @VisibleForTesting
  static final String IDLE_PRIMARY_DEVICE_ALERT = "idle-primary-device";

  @VisibleForTesting
  static final String CRITICAL_IDLE_PRIMARY_DEVICE_ALERT = "critical-idle-primary-device";

  @VisibleForTesting
  static final Duration PQ_KEY_CHECK_THRESHOLD = Duration.ofDays(120);

  private static final Counter IDLE_PRIMARY_WARNING_COUNTER = Metrics.counter(
      MetricsUtil.name(IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter.class, "idlePrimaryDeviceWarning"),
      "critical", "false");

  private static final Counter CRITICAL_IDLE_PRIMARY_WARNING_COUNTER = Metrics.counter(
      MetricsUtil.name(IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter.class, "idlePrimaryDeviceWarning"),
      "critical", "true");

  public IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter(final KeysManager keysManager,
      final Duration minIdleDuration,
      final Clock clock) {

    this.keysManager = keysManager;
    this.minIdleDuration = minIdleDuration;
    this.clock = clock;
  }

  @Override
  public void handleAuthentication(final Optional<AuthenticatedDevice> authenticated,
      final JettyServerUpgradeRequest request,
      final JettyServerUpgradeResponse response) {

    // No action needed if the connection is unauthenticated (in which case we don't know when we've last seen the
    // primary device) or if the authenticated device IS the primary device
    authenticated
        .filter(authenticatedDevice -> authenticatedDevice.getDeviceId() != Device.PRIMARY_ID)
        .ifPresent(authenticatedDevice -> {
          final Instant primaryDeviceLastSeen = authenticatedDevice.getPrimaryDeviceLastSeen();

          if (primaryDeviceLastSeen.isBefore(clock.instant().minus(PQ_KEY_CHECK_THRESHOLD)) &&
              keysManager.getLastResort(authenticatedDevice.getAccountIdentifier(), Device.PRIMARY_ID).join().isEmpty()) {

            response.addHeader(ALERT_HEADER, CRITICAL_IDLE_PRIMARY_DEVICE_ALERT);
            CRITICAL_IDLE_PRIMARY_WARNING_COUNTER.increment();
          } else if (primaryDeviceLastSeen.isBefore(clock.instant().minus(minIdleDuration))) {
            response.addHeader(ALERT_HEADER, IDLE_PRIMARY_DEVICE_ALERT);
            IDLE_PRIMARY_WARNING_COUNTER.increment();
          }
        });
  }
}
