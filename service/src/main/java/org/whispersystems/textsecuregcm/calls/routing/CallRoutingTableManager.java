/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.S3ObjectMonitorFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.s3.S3ObjectMonitor;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class CallRoutingTableManager implements Supplier<CallRoutingTable>, Managed {

  private final S3ObjectMonitor objectMonitor;

  private final AtomicReference<CallRoutingTable> routingTable = new AtomicReference<>();

  private final String tableTag;

  private final Timer refreshTimer;

  private static final Logger log = LoggerFactory.getLogger(CallRoutingTableManager.class);

  public CallRoutingTableManager(final ScheduledExecutorService executorService,
      final AwsCredentialsProvider awsCredentialsProvider, final S3ObjectMonitorFactory configuration,
      final String tableTag) {

    this.objectMonitor = configuration.build(awsCredentialsProvider, executorService);
    this.tableTag = tableTag;
    this.routingTable.set(CallRoutingTable.empty());
    this.refreshTimer = Metrics.timer(MetricsUtil.name(CallRoutingTableManager.class, tableTag));
  }

  private void handleDatabaseChanged(final InputStream inputStream) {
    refreshTimer.record(() -> {
      try(InputStreamReader reader = new InputStreamReader(inputStream)) {
        CallRoutingTable newTable = CallRoutingTableParser.fromJson(reader);
        this.routingTable.set(newTable);
        log.info("Replaced {} call routing table: {}", tableTag, newTable.toSummaryString());
      } catch (final IOException e) {
        log.error("Failed to parse and update {} call routing table", tableTag);
      }
    });
  }

  @Override
  public void start() throws Exception {
    objectMonitor.start(this::handleDatabaseChanged);
  }

  @Override
  public void stop() throws Exception {
    objectMonitor.stop();
    routingTable.getAndSet(null);
  }

  @Override
  public CallRoutingTable get() {
    return this.routingTable.get();
  }
}
