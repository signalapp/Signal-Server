/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;

public class FreeMemoryGauge implements Gauge {

  private final OperatingSystemMXBean operatingSystemMXBean;

  public FreeMemoryGauge() {
    this.operatingSystemMXBean = (com.sun.management.OperatingSystemMXBean)
        ManagementFactory.getOperatingSystemMXBean();
  }

  @Override
  public double getValue() {
    return operatingSystemMXBean.getFreeMemorySize();
  }
}
