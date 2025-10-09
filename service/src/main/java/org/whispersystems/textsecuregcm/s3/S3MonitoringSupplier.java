/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.s3;

import static java.util.Objects.requireNonNull;
import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.S3ObjectMonitorFactory;
import org.whispersystems.textsecuregcm.s3.ManagedSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class S3MonitoringSupplier<T> implements ManagedSupplier<T> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Timer refreshTimer;

  private final Counter refreshErrors;

  private final AtomicReference<T> holder;

  private final S3ObjectMonitor monitor;

  private final Function<InputStream, T> parser;


  public S3MonitoringSupplier(
      final ScheduledExecutorService executor,
      final AwsCredentialsProvider awsCredentialsProvider,
      final S3ObjectMonitorFactory cfg,
      final Function<InputStream, T> parser,
      final T initial,
      final String name) {
    this.refreshTimer = Metrics.timer(name(S3MonitoringSupplier.class, name, "refresh"));
    this.refreshErrors = Metrics.counter(name(S3MonitoringSupplier.class, name, "refreshErrors"));
    this.holder = new AtomicReference<>(initial);
    this.parser = requireNonNull(parser);
    this.monitor = cfg.build(awsCredentialsProvider, executor);
  }

  @Override
  public T get() {
    return requireNonNull(holder.get());
  }

  @Override
  public void start() throws Exception {
    monitor.start(this::handleObjectChange);
  }

  @Override
  public void stop() throws Exception {
    monitor.stop();
  }

  private void handleObjectChange(final InputStream inputStream) {
    refreshTimer.record(() -> {
      // parser function is supposed to close the input stream
      try {
        holder.set(parser.apply(inputStream));
      } catch (final Exception e) {
        log.error("failed to update internal state from the monitored object", e);
        refreshErrors.increment();
      }
    });
  }
}
