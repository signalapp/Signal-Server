package org.whispersystems.textsecuregcm.metrics;

import com.sun.management.OperatingSystemMXBean;
import com.yammer.metrics.core.Gauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;

public class FreeMemoryGauge extends Gauge<Long> {

  @Override
  public Long value() {
    OperatingSystemMXBean mbean = (com.sun.management.OperatingSystemMXBean)
        ManagementFactory.getOperatingSystemMXBean();

    return mbean.getFreePhysicalMemorySize();
  }
}