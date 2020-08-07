package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.Gauge;
import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

public class CpuUsageGauge implements Gauge<Integer> {

  private final OperatingSystemMXBean operatingSystemMXBean;

  public CpuUsageGauge() {
    this.operatingSystemMXBean = (com.sun.management.OperatingSystemMXBean)
            ManagementFactory.getOperatingSystemMXBean();
  }

  @Override
  public Integer getValue() {
    return (int) Math.ceil(operatingSystemMXBean.getSystemCpuLoad() * 100);
  }
}
