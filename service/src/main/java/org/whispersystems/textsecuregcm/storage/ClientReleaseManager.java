/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.vdurmont.semver4j.Semver;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import javax.annotation.Nullable;

public class ClientReleaseManager implements Managed {

  private final ClientReleases clientReleases;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Duration refreshInterval;
  private final Clock clock;

  @Nullable
  private ScheduledFuture<?> refreshClientReleasesFuture;

  private volatile Map<ClientPlatform, Map<Semver, ClientRelease>> clientReleasesByPlatform = Collections.emptyMap();

  private static final Logger logger = LoggerFactory.getLogger(ClientReleaseManager.class);

  public ClientReleaseManager(final ClientReleases clientReleases,
      final ScheduledExecutorService scheduledExecutorService,
      final Duration refreshInterval,
      final Clock clock) {

    this.clientReleases = clientReleases;
    this.scheduledExecutorService = scheduledExecutorService;
    this.refreshInterval = refreshInterval;
    this.clock = clock;
  }

  public boolean isVersionActive(final ClientPlatform platform, final Semver version) {
    final Map<Semver, ClientRelease> releasesByVersion = clientReleasesByPlatform.get(platform);

    return releasesByVersion != null &&
        releasesByVersion.containsKey(version) &&
        releasesByVersion.get(version).expiration().isAfter(clock.instant());
  }

  @Override
  public void start() throws Exception {
    refreshClientVersions();

    refreshClientReleasesFuture =
        scheduledExecutorService.scheduleWithFixedDelay(this::refreshClientVersions,
            refreshInterval.toMillis(),
            refreshInterval.toMillis(),
            TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() throws Exception {
    if (refreshClientReleasesFuture != null) {
      refreshClientReleasesFuture.cancel(true);
    }
  }

  void refreshClientVersions() {
    try {
      clientReleasesByPlatform = clientReleases.getClientReleases();

      logger.debug("Loaded client releases; android: {}, desktop: {}, ios: {}",
          clientReleasesByPlatform.getOrDefault(ClientPlatform.ANDROID, Collections.emptyMap()).size(),
          clientReleasesByPlatform.getOrDefault(ClientPlatform.DESKTOP, Collections.emptyMap()).size(),
          clientReleasesByPlatform.getOrDefault(ClientPlatform.IOS, Collections.emptyMap()).size());
    } catch (final Exception e) {
      logger.warn("Failed to refresh client releases", e);
    }
  }
}
