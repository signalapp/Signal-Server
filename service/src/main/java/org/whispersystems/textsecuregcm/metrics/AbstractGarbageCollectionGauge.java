package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.Gauge;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

abstract class AbstractGarbageCollectionGauge implements Gauge<Long> {

    private final GarbageCollectorMXBean garbageCollectorMXBean;

    public AbstractGarbageCollectionGauge() {
        this.garbageCollectorMXBean = (GarbageCollectorMXBean)ManagementFactory.getGarbageCollectorMXBeans();
    }

    protected GarbageCollectorMXBean getGarbageCollectorMXBean() {
        return garbageCollectorMXBean;
    }
}
