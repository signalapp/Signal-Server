/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.vdurmont.semver4j.Semver;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

class ClientReleaseManagerTest {

  private ClientReleases clientReleases;
  private Clock clock;

  private ClientReleaseManager clientReleaseManager;

  @BeforeEach
  void setUp() {
    clientReleases = mock(ClientReleases.class);
    clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

    clientReleaseManager =
        new ClientReleaseManager(clientReleases, mock(ScheduledExecutorService.class), Duration.ofHours(4), clock);
  }

  @Test
  void isVersionActive() {
    final Semver iosVersion = new Semver("1.2.3");
    final Semver desktopVersion = new Semver("4.5.6");

    when(clientReleases.getClientReleases()).thenReturn(Map.of(
        ClientPlatform.DESKTOP, Map.of(desktopVersion, new ClientRelease(ClientPlatform.DESKTOP, desktopVersion, clock.instant(), clock.instant().plus(Duration.ofDays(90)))),
        ClientPlatform.IOS, Map.of(iosVersion, new ClientRelease(ClientPlatform.IOS, iosVersion, clock.instant().minus(Duration.ofDays(91)), clock.instant().minus(Duration.ofDays(1))))
    ));

    clientReleaseManager.refreshClientVersions();

    assertTrue(clientReleaseManager.isVersionActive(ClientPlatform.DESKTOP, desktopVersion));
    assertFalse(clientReleaseManager.isVersionActive(ClientPlatform.DESKTOP, iosVersion));
    assertFalse(clientReleaseManager.isVersionActive(ClientPlatform.IOS, iosVersion));
    assertFalse(clientReleaseManager.isVersionActive(ClientPlatform.ANDROID, new Semver("7.8.9")));
  }
}
