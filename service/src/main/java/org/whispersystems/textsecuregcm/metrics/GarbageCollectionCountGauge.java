package org.whispersystems.textsecuregcm.metrics;

/**
 * A gauge that reports the total number of collections that have occurred in this JVM's lifetime.
 */
public class GarbageCollectionCountGauge extends AbstractGarbageCollectionGauge {

    @Override
    public Long getValue() {
        return getGarbageCollectorMXBean().getCollectionCount();
    }
}
