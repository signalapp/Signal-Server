/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;


import static com.codahale.metrics.MetricRegistry.name;

import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A managed monitor that reports whether the application is shutting down as a metric. That metric can then be used in
 * conjunction with other indicators to conditionally fire or suppress alerts.
 */
public class ApplicationShutdownMonitor implements Managed {

  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

  public ApplicationShutdownMonitor(final MeterRegistry meterRegistry) {
    // without a strong reference to the gauge’s value supplier, shutdown garbage collection
    // might prevent the final value from being reported
    Gauge.builder(name(getClass().getSimpleName(), "shuttingDown"), () -> shuttingDown.get() ? 1 : 0)
        .strongReference(true)
        .register(meterRegistry);
  }

  @Override
  public void start() throws Exception {
    shuttingDown.set(false);
  }

  @Override
  public void stop() throws Exception {
    shuttingDown.set(true);
  }
}
