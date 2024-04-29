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
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.S3ObjectMonitorFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.s3.S3ObjectMonitor;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class CallDnsRecordsManager implements Supplier<CallDnsRecords>, Managed {

  private final S3ObjectMonitor objectMonitor;

  private final AtomicReference<CallDnsRecords> callDnsRecords = new AtomicReference<>();

  private final Timer refreshTimer;

  private static final Logger log = LoggerFactory.getLogger(CallDnsRecordsManager.class);

  private static final ObjectMapper objectMapper = JsonMapper.builder()
      .enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
      .build();

  public CallDnsRecordsManager(final ScheduledExecutorService executorService,
      final AwsCredentialsProvider awsCredentialsProvider, final S3ObjectMonitorFactory configuration) {

    this.objectMonitor = configuration.build(awsCredentialsProvider, executorService);
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
    objectMonitor.start(this::handleDatabaseChanged);
  }

  @Override
  public void stop() throws Exception {
    objectMonitor.stop();
    callDnsRecords.getAndSet(null);
  }

  @Override
  public CallDnsRecords get() {
    return this.callDnsRecords.get();
  }
}
