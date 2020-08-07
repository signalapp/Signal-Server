package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.Gauge;
import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

public class FreeMemoryGauge implements Gauge<Long> {

  private final OperatingSystemMXBean operatingSystemMXBean;

  public FreeMemoryGauge() {
    this.operatingSystemMXBean = (com.sun.management.OperatingSystemMXBean)
            ManagementFactory.getOperatingSystemMXBean();
  }

  @Override
  public Long getValue() {
    return operatingSystemMXBean.getFreePhysicalMemorySize();
  }
}
