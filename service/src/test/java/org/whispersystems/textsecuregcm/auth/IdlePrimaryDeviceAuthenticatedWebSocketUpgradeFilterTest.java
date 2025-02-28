/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import javax.annotation.Nullable;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeRequest;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.websocket.ReusableAuth;
import org.whispersystems.websocket.auth.PrincipalSupplier;

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
  void handleAuthentication(@Nullable final AuthenticatedDevice authenticatedDevice, final boolean expectAlertHeader) {
    final ReusableAuth<AuthenticatedDevice> reusableAuth = authenticatedDevice != null
        ? ReusableAuth.authenticated(authenticatedDevice, PrincipalSupplier.forImmutablePrincipal())
        : ReusableAuth.anonymous();

    final JettyServerUpgradeResponse response = mock(JettyServerUpgradeResponse.class);

    filter.handleAuthentication(reusableAuth, mock(JettyServerUpgradeRequest.class), response);

    if (expectAlertHeader) {
      verify(response).addHeader(IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter.ALERT_HEADER,
          IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter.IDLE_PRIMARY_DEVICE_ALERT);
    } else {
      verifyNoInteractions(response);
    }
  }

  private static List<Arguments> handleAuthentication() {
    final Device activePrimaryDevice = mock(Device.class);
    when(activePrimaryDevice.isPrimary()).thenReturn(true);
    when(activePrimaryDevice.getLastSeen()).thenReturn(CLOCK.millis());

    final Device idlePrimaryDevice = mock(Device.class);
    when(idlePrimaryDevice.isPrimary()).thenReturn(true);
    when(idlePrimaryDevice.getLastSeen())
        .thenReturn(CLOCK.instant().minus(MIN_IDLE_DURATION).minusSeconds(1).toEpochMilli());

    final Device linkedDevice = mock(Device.class);
    when(linkedDevice.isPrimary()).thenReturn(false);

    final Account accountWithActivePrimaryDevice = mock(Account.class);
    when(accountWithActivePrimaryDevice.getPrimaryDevice()).thenReturn(activePrimaryDevice);

    final Account accountWithIdlePrimaryDevice = mock(Account.class);
    when(accountWithIdlePrimaryDevice.getPrimaryDevice()).thenReturn(idlePrimaryDevice);

    return List.of(
        Arguments.argumentSet("Anonymous",
            null,
            false),

        Arguments.argumentSet("Authenticated as active primary device",
            new AuthenticatedDevice(accountWithActivePrimaryDevice, activePrimaryDevice),
            false),

        Arguments.argumentSet("Authenticated as idle primary device",
            new AuthenticatedDevice(accountWithIdlePrimaryDevice, idlePrimaryDevice),
            false),

        Arguments.argumentSet("Authenticated as linked device with active primary device",
            new AuthenticatedDevice(accountWithActivePrimaryDevice, linkedDevice),
            false),

        Arguments.argumentSet("Authenticated as linked device with idle primary device",
            new AuthenticatedDevice(accountWithIdlePrimaryDevice, linkedDevice),
            true)
    );
  }
}
