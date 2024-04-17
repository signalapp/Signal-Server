/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.sun.management.OperatingSystemMXBean;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.lang.management.ManagementFactory;

public class FreeMemoryGauge implements MeterBinder {

  private final OperatingSystemMXBean operatingSystemMXBean;

  public FreeMemoryGauge() {
    this.operatingSystemMXBean = (com.sun.management.OperatingSystemMXBean)
        ManagementFactory.getOperatingSystemMXBean();
  }

  @Override
  public void bindTo(final MeterRegistry registry) {
    Gauge.builder(name(FreeMemoryGauge.class, "freeMemory"), operatingSystemMXBean,
            OperatingSystemMXBean::getFreeMemorySize)
        .register(registry);

  }
}
