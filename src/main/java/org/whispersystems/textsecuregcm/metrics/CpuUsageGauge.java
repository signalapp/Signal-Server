package org.whispersystems.textsecuregcm.metrics;

import com.sun.management.OperatingSystemMXBean;
import com.yammer.metrics.core.Gauge;

import java.lang.management.ManagementFactory;

public class CpuUsageGauge extends Gauge<Integer> {
  @Override
  public Integer value() {
    OperatingSystemMXBean mbean = (com.sun.management.OperatingSystemMXBean)
        ManagementFactory.getOperatingSystemMXBean();

    return (int) Math.ceil(mbean.getSystemCpuLoad() * 100);
  }
}
