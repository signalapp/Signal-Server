/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.MonitoredS3ObjectConfiguration;

/**
 * A utility for checking whether IP addresses belong to Tor exit nodes using the "bulk exit list."
 *
 * @see <a href="https://blog.torproject.org/changes-tor-exit-list-service">Changes to the Tor Exit List Service</a>
 */
public class TorExitNodeManager implements Managed {

  private final S3ObjectMonitor exitListMonitor;

  private final AtomicReference<Set<String>> exitNodeAddresses = new AtomicReference<>(Collections.emptySet());

  private static final Timer REFRESH_TIMER = Metrics.timer(name(TorExitNodeManager.class, "refresh"));
  private static final Counter REFRESH_ERRORS = Metrics.counter(name(TorExitNodeManager.class, "refreshErrors"));

  private static final Logger log = LoggerFactory.getLogger(TorExitNodeManager.class);

  public TorExitNodeManager(
      final ScheduledExecutorService scheduledExecutorService,
      final MonitoredS3ObjectConfiguration configuration) {

    this.exitListMonitor = new S3ObjectMonitor(
        configuration.getS3Region(),
        configuration.getS3Bucket(),
        configuration.getObjectKey(),
        configuration.getMaxSize(),
        scheduledExecutorService,
        configuration.getRefreshInterval(),
        this::handleExitListChanged);
  }

  @Override
  public synchronized void start() {
    exitListMonitor.start();
  }

  @Override
  public synchronized void stop() {
    exitListMonitor.stop();
  }

  public boolean isTorExitNode(final String address) {
    return exitNodeAddresses.get().contains(address);
  }

  private void handleExitListChanged(final InputStream exitList) {
    REFRESH_TIMER.record(() -> handleExitListChangedStream(exitList));
  }

  @VisibleForTesting
  void handleExitListChangedStream(final InputStream inputStream) {
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      exitNodeAddresses.set(reader.lines().collect(Collectors.toSet()));
    } catch (final Exception e) {
      REFRESH_ERRORS.increment();
      log.warn("Failed to refresh Tor exit node list", e);
    }
  }
}
