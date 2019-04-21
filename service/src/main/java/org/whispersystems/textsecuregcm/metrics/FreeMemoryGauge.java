package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.Gauge;
import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

public class FreeMemoryGauge implements Gauge<Long> {

  @Override
  public Long getValue() {
    OperatingSystemMXBean mbean = (com.sun.management.OperatingSystemMXBean)
        ManagementFactory.getOperatingSystemMXBean();

    return mbean.getFreePhysicalMemorySize();
  }
}