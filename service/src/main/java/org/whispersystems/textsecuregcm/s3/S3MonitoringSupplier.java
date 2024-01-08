/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.s3;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.requireNonNull;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.MonitoredS3ObjectConfiguration;

public class S3MonitoringSupplier<T> implements ManagedSupplier<T> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Nonnull
  private final Timer refreshTimer;

  @Nonnull
  private final Counter refreshErrors;

  @Nonnull
  private final AtomicReference<T> holder;

  @Nonnull
  private final S3ObjectMonitor monitor;

  @Nonnull
  private final Function<InputStream, T> parser;


  public S3MonitoringSupplier(
      @Nonnull final ScheduledExecutorService executor,
      @Nonnull final MonitoredS3ObjectConfiguration cfg,
      @Nonnull final Function<InputStream, T> parser,
      @Nonnull final T initial,
      @Nonnull final String name) {
    this.refreshTimer = Metrics.timer(name(name, "refresh"));
    this.refreshErrors = Metrics.counter(name(name, "refreshErrors"));
    this.holder = new AtomicReference<>(initial);
    this.parser = requireNonNull(parser);
    this.monitor = new S3ObjectMonitor(
        cfg.s3Region(),
        cfg.s3Bucket(),
        cfg.objectKey(),
        cfg.maxSize(),
        executor,
        cfg.refreshInterval(),
        this::handleObjectChange
    );
  }

  @Override
  @Nonnull
  public T get() {
    return requireNonNull(holder.get());
  }

  @Override
  public void start() throws Exception {
    monitor.start();
  }

  @Override
  public void stop() throws Exception {
    monitor.stop();
  }

  private void handleObjectChange(@Nonnull final InputStream inputStream) {
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
