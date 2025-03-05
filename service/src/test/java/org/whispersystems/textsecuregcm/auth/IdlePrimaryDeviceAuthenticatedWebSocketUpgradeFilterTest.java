/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeRequest;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.websocket.ReusableAuth;
import org.whispersystems.websocket.auth.PrincipalSupplier;

class IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilterTest {

  private KeysManager keysManager;

  private IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter filter;

  private static final Duration MIN_IDLE_DURATION =
      IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter.PQ_KEY_CHECK_THRESHOLD.dividedBy(2);

  private static final TestClock CLOCK = TestClock.pinned(Instant.now());

  @BeforeEach
  void setUp() {
    keysManager = mock(KeysManager.class);

    filter = new IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter(keysManager, MIN_IDLE_DURATION, CLOCK);
  }

  @ParameterizedTest
  @MethodSource
  void handleAuthentication(@Nullable final AuthenticatedDevice authenticatedDevice,
      final boolean primaryDeviceHasPqKeys,
      final boolean expectPqKeyCheck,
      @Nullable final String expectedAlertHeader) {

    final ReusableAuth<AuthenticatedDevice> reusableAuth = authenticatedDevice != null
        ? ReusableAuth.authenticated(authenticatedDevice, PrincipalSupplier.forImmutablePrincipal())
        : ReusableAuth.anonymous();

    final JettyServerUpgradeResponse response = mock(JettyServerUpgradeResponse.class);

    when(keysManager.getLastResort(any(), eq(Device.PRIMARY_ID)))
        .thenReturn(CompletableFuture.completedFuture(primaryDeviceHasPqKeys
            ? Optional.of(mock(KEMSignedPreKey.class))
            : Optional.empty()));

    filter.handleAuthentication(reusableAuth, mock(JettyServerUpgradeRequest.class), response);

    if (expectPqKeyCheck) {
      verify(keysManager).getLastResort(any(), eq(Device.PRIMARY_ID));
    } else {
      verify(keysManager, never()).getLastResort(any(), anyByte());
    }

    if (expectedAlertHeader != null) {
      verify(response)
          .addHeader(IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter.ALERT_HEADER, expectedAlertHeader);
    } else {
      verifyNoInteractions(response);
    }
  }

  private static List<Arguments> handleAuthentication() {
    final Device activePrimaryDevice = mock(Device.class);
    when(activePrimaryDevice.isPrimary()).thenReturn(true);
    when(activePrimaryDevice.getLastSeen()).thenReturn(CLOCK.millis());

    final Device minIdlePrimaryDevice = mock(Device.class);
    when(minIdlePrimaryDevice.isPrimary()).thenReturn(true);
    when(minIdlePrimaryDevice.getLastSeen())
        .thenReturn(CLOCK.instant().minus(MIN_IDLE_DURATION).minusSeconds(1).toEpochMilli());

    final Device longIdlePrimaryDevice = mock(Device.class);
    when(longIdlePrimaryDevice.isPrimary()).thenReturn(true);
    when(longIdlePrimaryDevice.getLastSeen())
        .thenReturn(CLOCK.instant().minus(IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter.PQ_KEY_CHECK_THRESHOLD).minusSeconds(1).toEpochMilli());

    final Device linkedDevice = mock(Device.class);
    when(linkedDevice.isPrimary()).thenReturn(false);

    final Account accountWithActivePrimaryDevice = mock(Account.class);
    when(accountWithActivePrimaryDevice.getPrimaryDevice()).thenReturn(activePrimaryDevice);

    final Account accountWithMinIdlePrimaryDevice = mock(Account.class);
    when(accountWithMinIdlePrimaryDevice.getPrimaryDevice()).thenReturn(minIdlePrimaryDevice);

    final Account accountWithLongIdlePrimaryDevice = mock(Account.class);
    when(accountWithLongIdlePrimaryDevice.getPrimaryDevice()).thenReturn(longIdlePrimaryDevice);

    return List.of(
        Arguments.argumentSet("Anonymous",
            null,
            true,
            false,
            null),

        Arguments.argumentSet("Authenticated as active primary device",
            new AuthenticatedDevice(accountWithActivePrimaryDevice, activePrimaryDevice),
            true,
            false,
            null),

        Arguments.argumentSet("Authenticated as idle primary device",
            new AuthenticatedDevice(accountWithMinIdlePrimaryDevice, minIdlePrimaryDevice),
            true,
            false,
            null),

        Arguments.argumentSet("Authenticated as linked device with active primary device",
            new AuthenticatedDevice(accountWithActivePrimaryDevice, linkedDevice),
            true,
            false,
            null),

        Arguments.argumentSet("Authenticated as linked device with min-idle primary device",
            new AuthenticatedDevice(accountWithMinIdlePrimaryDevice, linkedDevice),
            true,
            false,
            IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter.IDLE_PRIMARY_DEVICE_ALERT),

        Arguments.argumentSet("Authenticated as linked device with long-idle primary device with PQ keys",
            new AuthenticatedDevice(accountWithLongIdlePrimaryDevice, linkedDevice),
            true,
            true,
            IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter.IDLE_PRIMARY_DEVICE_ALERT),

        Arguments.argumentSet("Authenticated as linked device with long-idle primary device without PQ keys",
            new AuthenticatedDevice(accountWithLongIdlePrimaryDevice, linkedDevice),
            false,
            true,
            IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter.CRITICAL_IDLE_PRIMARY_DEVICE_ALERT)
    );
  }
}
