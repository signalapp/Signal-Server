package org.whispersystems.textsecuregcm.metrics;

/**
 * A gauge that reports the cumulative amount of time (in milliseconds) spent on garbage collection.
 */
public class GarbageCollectionTimeGauge extends AbstractGarbageCollectionGauge {

    @Override
    public Long getValue() {
        return getGarbageCollectorMXBean().getCollectionTime();
    }
}
