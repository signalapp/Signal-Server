/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeRequest;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.TestClock;

class IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilterTest {

  private IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter filter;

  private static final Duration MIN_IDLE_DURATION = Duration.ofDays(30);

  private static final TestClock CLOCK = TestClock.pinned(Instant.now());

  @BeforeEach
  void setUp() {
    filter = new IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter(MIN_IDLE_DURATION, CLOCK);
  }

  @ParameterizedTest
  @MethodSource
  void handleAuthentication(@Nullable final AuthenticatedDevice authenticatedDevice,
      @Nullable final String expectedAlertHeader) {

    final Optional<AuthenticatedDevice> reusableAuth = authenticatedDevice != null
        ? Optional.of(authenticatedDevice)
        : Optional.empty();

    final JettyServerUpgradeResponse response = mock(JettyServerUpgradeResponse.class);

    filter.handleAuthentication(reusableAuth, mock(JettyServerUpgradeRequest.class), response);

    if (expectedAlertHeader != null) {
      verify(response).addHeader(IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter.ALERT_HEADER, expectedAlertHeader);
    } else {
      verifyNoInteractions(response);
    }
  }

  private static List<Arguments> handleAuthentication() {
    final Instant activePrimaryDeviceLastSeen = CLOCK.instant();
    final Instant idlePrimaryDeviceLastSeen = CLOCK.instant().minus(MIN_IDLE_DURATION).minusSeconds(1);

    return List.of(
        Arguments.argumentSet("Anonymous",
            null,
            null),

        Arguments.argumentSet("Authenticated as active primary device",
            new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID, activePrimaryDeviceLastSeen),
            null),

        Arguments.argumentSet("Authenticated as idle primary device",
            new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID, idlePrimaryDeviceLastSeen),
            null),

        Arguments.argumentSet("Authenticated as linked device with active primary device",
            new AuthenticatedDevice(UUID.randomUUID(), (byte) (Device.PRIMARY_ID + 1), activePrimaryDeviceLastSeen),
            null),

        Arguments.argumentSet("Authenticated as linked device with idle primary device",
            new AuthenticatedDevice(UUID.randomUUID(), (byte) (Device.PRIMARY_ID + 1), idlePrimaryDeviceLastSeen),
            IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter.IDLE_PRIMARY_DEVICE_ALERT)
    );
  }
}
