/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.whispersystems.textsecuregcm.configuration.MonitoredS3ObjectConfiguration;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.s3.S3ObjectMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nonnull;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class CallDnsRecordsManager implements Supplier<CallDnsRecords>, Managed {

  private final S3ObjectMonitor objectMonitor;

  private final AtomicReference<CallDnsRecords> callDnsRecords = new AtomicReference<>();

  private final Timer refreshTimer;

  private static final Logger log = LoggerFactory.getLogger(CallDnsRecordsManager.class);

  private static final ObjectMapper objectMapper = JsonMapper.builder()
      .enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
      .build();

  public CallDnsRecordsManager(
      @Nonnull final ScheduledExecutorService executorService,
      @Nonnull final MonitoredS3ObjectConfiguration configuration
  ){
    this.objectMonitor = new S3ObjectMonitor(
        configuration.s3Region(),
        configuration.s3Bucket(),
        configuration.objectKey(),
        configuration.maxSize(),
        executorService,
        configuration.refreshInterval(),
        this::handleDatabaseChanged
    );

    this.callDnsRecords.set(CallDnsRecords.empty());
    this.refreshTimer = Metrics.timer(MetricsUtil.name(CallDnsRecordsManager.class, "refresh"));
  }

  private void handleDatabaseChanged(final InputStream inputStream) {
    refreshTimer.record(() -> {
      try (final InputStream bufferedInputStream = new BufferedInputStream(inputStream)) {
        final CallDnsRecords newRecords = parseRecords(bufferedInputStream);
        final CallDnsRecords oldRecords = callDnsRecords.getAndSet(newRecords);
        log.info("Replaced dns records, old summary=[{}], new summary=[{}]", oldRecords != null ? oldRecords.getSummary() : "null", newRecords);
      } catch (final IOException e) {
        log.error("Failed to load Call DNS Records");
      }
    });
  }

  static CallDnsRecords parseRecords(InputStream inputStream) throws IOException {
    return objectMapper.readValue(inputStream, CallDnsRecords.class);
  }

  @Override
  public void start() throws Exception {
    Managed.super.start();
    objectMonitor.start();
  }

  @Override
  public void stop() throws Exception {
    objectMonitor.stop();
    Managed.super.stop();
    callDnsRecords.getAndSet(null);
  }

  @Override
  public CallDnsRecords get() {
    return this.callDnsRecords.get();
  }
}
