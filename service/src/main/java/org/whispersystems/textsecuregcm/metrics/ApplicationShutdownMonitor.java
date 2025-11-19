/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;


import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.thread.ShutdownThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A managed monitor that reports whether the application is shutting down as a metric. That metric can then be used in
 * conjunction with other indicators to conditionally fire or suppress alerts.
 */
public class ApplicationShutdownMonitor extends AbstractLifeCycle {

  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private final Logger logger = LoggerFactory.getLogger(ApplicationShutdownMonitor.class);

  public ApplicationShutdownMonitor(final MeterRegistry meterRegistry) {
    // without a strong reference to the gauge’s value supplier, shutdown garbage collection
    // might prevent the final value from being reported
    Gauge.builder(name(getClass(), "shuttingDown"), () -> shuttingDown.get() ? 1 : 0)
        .strongReference(true)
        .register(meterRegistry);
  }

  public void register() {
    // Force this component to get shut down before Dropwizard's
    // DelayedShutdownHandler, which initiates the delayed-shutdown process
    // without an additional chance for us to hook it
    logger.info("registering shutdown monitor");
    try {
      start();                    // jetty won't stop an unstarted lifecycle
      ShutdownThread.register(0, this);
    } catch (Exception e) {
      logger.error("failed to start application shutdown monitor", e);
    }
  }

  @Override
  public void doStop() {
    logger.info("setting shutdown flag");
    shuttingDown.set(true);
  }
}
