/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.MonitoredS3ObjectConfiguration;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class AsnManager implements Managed {

  private final S3ObjectMonitor asnTableMonitor;

  private final AtomicReference<AsnTable> asnTable = new AtomicReference<>(AsnTable.EMPTY);

  private static final Timer REFRESH_TIMER = Metrics.timer(name(AsnManager.class, "refresh"));
  private static final Counter REFRESH_ERRORS = Metrics.counter(name(AsnManager.class, "refreshErrors"));

  private static final Logger log = LoggerFactory.getLogger(AsnManager.class);

  public AsnManager(
      final ScheduledExecutorService scheduledExecutorService,
      final MonitoredS3ObjectConfiguration configuration) {

    this.asnTableMonitor = new S3ObjectMonitor(
        configuration.getS3Region(),
        configuration.getS3Bucket(),
        configuration.getObjectKey(),
        configuration.getMaxSize(),
        scheduledExecutorService,
        configuration.getRefreshInterval(),
        this::handleAsnTableChanged);
  }

  @Override
  public void start() throws Exception {
    asnTableMonitor.start();
  }

  @Override
  public void stop() throws Exception {
    asnTableMonitor.stop();
  }

  public Optional<Long> getAsn(final String address) {
    try {
      return asnTable.get().getAsn((Inet4Address) Inet4Address.getByName(address));
    } catch (final UnknownHostException e) {
      log.warn("Could not parse \"{}\" as an Inet4Address", address);
      return Optional.empty();
    }
  }

  private void handleAsnTableChanged(final ResponseInputStream<GetObjectResponse> asnTableObject) {
    REFRESH_TIMER.record(() -> {
      try {
        handleAsnTableChangedStream(new GZIPInputStream(asnTableObject));
      } catch (final IOException e) {
        log.error("Retrieved object was not a gzip archive", e);
      }
    });
  }

  @VisibleForTesting
  void handleAsnTableChangedStream(final InputStream inputStream) {
    try (final InputStreamReader reader = new InputStreamReader(inputStream)) {
      asnTable.set(new AsnTable(reader));
    } catch (final Exception e) {
      REFRESH_ERRORS.increment();
      log.warn("Failed to refresh IP-to-ASN table", e);
    }
  }
}
